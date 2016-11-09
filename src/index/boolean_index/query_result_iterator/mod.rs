use std::cell::RefCell;

use index::boolean_index::boolean_query::*;
use index::boolean_index::posting::Posting;
use index::boolean_index::query_result_iterator::nary_query_iterator::*;
use utils::owning_iterator::{OwningIterator, SeekingIterator, ArcIter};

pub mod nary_query_iterator;

// The BooleanIndex implementation works with query iterators only. Why?
// 1. It is faster (no stack or heap allocation)
// 2. It is lazy
/// Wrapper around different query iterator types
/// Used to be able to simply and elegantly use nested queries of different
/// types
pub enum QueryResultIterator {
    Empty,
    Atom(usize, ArcIter<Posting>),
    NAry(NAryQueryIterator),
    Filter(FilterIterator),
}


impl Iterator for QueryResultIterator {
    type Item = u64;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_id()
    }
}

impl<'a> OwningIterator<'a> for QueryResultIterator {
    type Item = &'a Posting;


    fn next(&'a self) -> Option<&'a Posting> {
        match *self {
            QueryResultIterator::Empty => None,
            QueryResultIterator::Atom(_, ref iter) => iter.next(),
            QueryResultIterator::NAry(ref iter) => iter.next(),
            QueryResultIterator::Filter(ref iter) => iter.next(),
        }
    }


    /// Allows peeking. Used for union queries,
    /// which need to advance operands in some cases and peek in others
    fn peek(&'a self) -> Option<&'a Posting> {
        match *self {
            QueryResultIterator::Empty => None,
            QueryResultIterator::Atom(_, ref iter) => iter.peek(),
            QueryResultIterator::NAry(ref iter) => iter.peek(),
            QueryResultIterator::Filter(ref iter) => iter.peek(),
        }
    }


    fn len(&self) -> usize {
        self.estimate_length()
    }
}

impl<'a> SeekingIterator<'a> for QueryResultIterator {
    type Item = &'a Posting;

    fn next_seek(&'a self, target: Self::Item) -> Option<Self::Item> {
        match *self {
            QueryResultIterator::Empty => None,
            QueryResultIterator::Atom(_, ref iter) => iter.next_seek(target),
            QueryResultIterator::NAry(ref iter) => iter.next_seek(target),
            QueryResultIterator::Filter(ref iter) => iter.next_seek(target),
        }
    }

    fn peek_seek(&'a self, target: Self::Item) -> Option<Self::Item> {
        match *self {
            QueryResultIterator::Empty => None,
            QueryResultIterator::Atom(_, ref iter) => iter.peek_seek(target),
            QueryResultIterator::NAry(ref iter) => iter.peek_seek(target),
            QueryResultIterator::Filter(ref iter) => iter.peek_seek(target),
        }
    }
}

impl QueryResultIterator {
    pub fn next_id(&self) -> Option<u64> {
        self.next().map(|p| p.0)
    }

    /// Used to be able to sort queries according to their estimated number of
    /// results
    /// This can be used to optimize efficiency on intersecting queries
    fn estimate_length(&self) -> usize {
        match *self {
            QueryResultIterator::Empty => 0,
            QueryResultIterator::Atom(_, ref iter) => iter.len(),
            QueryResultIterator::NAry(ref iter) => iter.estimate_length(),
            QueryResultIterator::Filter(ref iter) => iter.estimate_length(),
        }
    }

    /// Return the relative position of a query-part in the whole query
    /// Necessary for positional queries
    fn relative_position(&self) -> usize {
        match *self {
            QueryResultIterator::Atom(rpos, _) => rpos,
            _ => 0,
        }

    }
}

pub struct FilterIterator {
    operator: FilterOperator,
    sand: Box<QueryResultIterator>,
    sieve: Box<QueryResultIterator>,
    peeked_value: RefCell<Option<Option<*const Posting>>>,
}

impl<'a> OwningIterator<'a> for FilterIterator {
    type Item = &'a Posting;

    fn next(&'a self) -> Option<&'a Posting> {
        let mut peeked_value = self.peeked_value.borrow_mut();
        if peeked_value.is_none() {
            match self.operator {
                FilterOperator::Not => self.next_not(),
            }
        } else {
            unsafe { peeked_value.take().unwrap().map(|p| &*p) }
        }
    }

    fn peek(&'a self) -> Option<&'a Posting> {
        let mut peeked_value = self.peeked_value.borrow_mut();
        if peeked_value.is_none() {
            *peeked_value = Some(self.next().map(|p| p as *const Posting));
        }
        unsafe { peeked_value.unwrap().map(|p| &*p) }
    }


    fn len(&self) -> usize {
        self.estimate_length()
    }
}

impl<'a> SeekingIterator<'a> for FilterIterator {
    type Item = &'a Posting;

    fn next_seek(&'a self, target: &Posting) -> Option<&'a Posting> {
        let mut peeked_value = self.peeked_value.borrow_mut();
        if peeked_value.is_some() {
            *peeked_value = None;
        }
        self.sand.peek_seek(target);
        self.next()
    }

    fn peek_seek(&'a self, target: &Posting) -> Option<&'a Posting> {
        let mut peeked_value = self.peeked_value.borrow_mut();
        if peeked_value.is_none() {
            *peeked_value = Some(self.next_seek(target).map(|p| p as *const Posting));
        }
        unsafe { peeked_value.unwrap().map(|p| &*p) }        
    }
}

impl FilterIterator {
    pub fn new(operator: FilterOperator, sand: Box<QueryResultIterator>, sieve: Box<QueryResultIterator>) -> Self {
        FilterIterator {
            operator: operator,
            sand: sand,
            sieve: sieve,
            peeked_value: RefCell::new(None),
        }
    }


    fn estimate_length(&self) -> usize {
        let sand_len = self.sand.estimate_length();
        let sieve_len = self.sieve.estimate_length();
        if sand_len > sieve_len {
            sand_len - sieve_len
        } else {
            0
        }
    }

    fn next_not(&self) -> Option<&Posting> {
        'sand: loop {
            // This slight inconvinience is there because of the conflicting implementations of
            // QueryResultIterator::next() (one for Iterator and one for OwningIterator)
            // TODO: Can we fix this?
            if let Some(sand) = OwningIterator::next(self.sand.as_ref()) {
                'sieve: loop {
                    if let Some(sieve) = self.sieve.peek_seek(sand) {
                        if sieve.0 == sand.0 {
                            continue 'sand;
                        }
                    }
                    return Some(sand);
                }
            }
            return None;
        }
    }
}

#[cfg(test)]
mod tests {
    use utils::owning_iterator::OwningIterator;

    use index::boolean_index::boolean_query::*;
    use index::boolean_index::tests::prepare_index;


    #[test]
    fn peek() {
        let index = prepare_index();
        let qri = index.run_atom(0, &0);
        assert!(qri.peek() == qri.peek());
        let qri2 = index.run_nary_query(&BooleanOperator::And,
                                        &vec![BooleanQuery::Atom(QueryAtom::new(0, 0)),
                                              BooleanQuery::Atom(QueryAtom::new(0, 0))]);
        assert!(qri2.peek() == qri2.peek());

    }

    #[test]
    fn estimate_length() {
        let index = prepare_index();
        assert!(index.run_atom(0, &0).estimate_length() == 3);
        assert!(index.run_atom(0, &3).estimate_length() == 2);
        assert!(index.run_atom(0, &16).estimate_length() == 1);
        assert!(index.run_nary_query(&BooleanOperator::And,
                            &vec![BooleanQuery::Atom(QueryAtom::new(0, 3)),
                                  BooleanQuery::Atom(QueryAtom::new(0, 16))])
            .estimate_length() == 1);
        assert!(index.run_nary_query(&BooleanOperator::Or,
                            &vec![BooleanQuery::Atom(QueryAtom::new(0, 3)),
                                  BooleanQuery::Atom(QueryAtom::new(0, 16))])
            .estimate_length() == 2);
    }

}
