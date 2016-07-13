use std::slice::Iter;
use std::iter::Iterator;
use std::iter::Peekable;

use index::boolean_index::*;
use index::boolean_index::posting::Posting;
use index::boolean_index::query_result_iterator::nary_query_iterator::*;

pub mod nary_query_iterator;

macro_rules! unwrap_or_return_none{
    ($operand:expr) => {
        if let Some(x) = $operand {
            x
        } else {
            return None;
        }
    }
}

/// Wrapper around different query iterator types
/// Used to be able to simply and elegantly use nested queries of different types
pub enum QueryResultIterator<'a> {
    Empty,
    Atom(usize, Peekable<Iter<'a, Posting>>),
    NAry(NAryQueryIterator<'a>),
    Filter(FilterIterator<'a>),
}

impl<'a> Iterator for QueryResultIterator<'a> {
    type Item = &'a Posting;

    fn next(&mut self) -> Option<&'a Posting> {
        match *self {
            QueryResultIterator::Empty => None,
            QueryResultIterator::Atom(_, ref mut iter) => iter.next(),
            QueryResultIterator::NAry(ref mut iter) => iter.next(),
            QueryResultIterator::Filter(ref mut iter) => iter.next()
        }
    }
}

impl<'a> QueryResultIterator<'a> {
    /// Used to be able to sort queries according to their estimated number of results
    /// This can be used to optimize efficiency on intersecting queries
    fn estimate_length(&self) -> usize {
        match *self {
            QueryResultIterator::Empty => 0,
            QueryResultIterator::Atom(_, ref iter) => iter.len(),
            QueryResultIterator::NAry(ref iter) => iter.estimate_length(),
            QueryResultIterator::Filter(ref iter) => iter.estimate_length()
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

    /// Allows peeking. Used for union queries,
    /// which need to advance operands in some cases and peek in others
    fn peek(&mut self) -> Option<&'a Posting> {
        match *self {
            QueryResultIterator::Empty => None,
            QueryResultIterator::Atom(_, ref mut iter) => iter.peek().map(|val| *val),
            QueryResultIterator::NAry(ref mut iter) => iter.peek(),
            QueryResultIterator::Filter(ref mut iter) => iter.peek()
        }
    }
}

pub struct FilterIterator<'a> {
    operator: FilterOperator,
    sand: Box<QueryResultIterator<'a>>,
    sieve: Box<QueryResultIterator<'a>>,
    peeked_value: Option<Option<&'a Posting>>,
}

impl<'a> Iterator for FilterIterator<'a> {
    type Item = &'a Posting;

    fn next(&mut self) -> Option<Self::Item> {
        if self.peeked_value.is_none() {
            match self.operator {
                FilterOperator::Not => self.next_not(),
            }
        } else {
            self.peeked_value.take().unwrap()
        }
    }   
}

impl<'a> FilterIterator<'a> {
    pub fn new(operator: FilterOperator, sand: Box<QueryResultIterator<'a>>, sieve: Box<QueryResultIterator<'a>>) -> Self {
        FilterIterator{
            operator: operator,
            sand: sand,
            sieve: sieve,
            peeked_value: None
        }
    }
        
    fn peek(&mut self) -> Option<&'a Posting> {
        if self.peeked_value.is_none() {
            self.peeked_value = Some(self.next())
        }
        self.peeked_value.unwrap()
    }

    fn estimate_length(&self) -> usize {
        let sand_len = self.sand.estimate_length();
        let sieve_len = self.sieve.estimate_length();
        if  sand_len > sieve_len {
            sand_len - sieve_len
        } else {
            0
        }
    }


    fn next_not(&mut self) -> Option<&'a Posting> {
        'sand: loop {
            if let Some(sand) = self.sand.next() {
                'sieve: loop {
                    if let Some(sieve) = self.sieve.peek() {
                        if sand.0 < sieve.0 {
                            return Some(sand);
                        } else if sand.0 > sieve.0 {
                            self.sieve.next();
                            continue 'sieve;
                        } else {
                            continue 'sand;
                        }
                    } else {
                        return Some(sand);
                    }
                }
            } else {
                return None;
            }
        }

    }
}

#[cfg(test)]
mod tests {
    use index::Index;
    use index::boolean_index::*;


    fn prepare_index() -> BooleanIndex<usize> {
        let mut index = BooleanIndex::new();
        index.index_document(0..10);
        index.index_document((0..10).map(|i| i * 2));
        index.index_document(vec![5, 4, 3, 2, 1, 0].into_iter());

        index
    }


    #[test]
    fn peek() {
        let index = prepare_index();
        let mut qri = index.run_atom(0, &0);
        assert!(qri.peek() == qri.peek());
        qri = index.run_nary_query(&BooleanOperator::And,
                                   &vec![BooleanQuery::Atom(QueryAtom::new(0, 0)),
                                         BooleanQuery::Atom(QueryAtom::new(0, 0))]);
        assert!(qri.peek() == qri.peek());

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
