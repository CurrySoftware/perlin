use index::boolean_index::boolean_query::*;
use index::boolean_index::posting::{PostingDecoder, Posting};
use index::boolean_index::query_result_iterator::nary_query_iterator::*;
use index::boolean_index::query_result_iterator::positional_query_iterator::*;
use utils::seeking_iterator::{PeekableSeekable, SeekingIterator};

pub mod nary_query_iterator;
pub mod positional_query_iterator;

// The BooleanIndex implementation works with query iterators only. Why?
// 1. It is faster (no stack or heap allocation)
// 2. It is lazy
/// Wrapper around different query iterator types
/// Used to be able to simply and elegantly use nested queries of different
/// types
pub enum QueryResultIterator<'a> {
    Empty,
    Atom(PostingDecoder<'a>),
    NAry(NAryQueryIterator<'a>),
    Positional(PositionalQueryIterator<'a>),
    Filter(FilterIterator<'a>),
}


impl<'a> Iterator for QueryResultIterator<'a> {
    type Item = Posting;


    fn next(&mut self) -> Option<Posting> {
        match *self {
            QueryResultIterator::Empty => None,
            QueryResultIterator::Atom(ref mut iter) => iter.next(),
            QueryResultIterator::NAry(ref mut iter) => iter.next(),
            QueryResultIterator::Filter(ref mut iter) => iter.next(),
            QueryResultIterator::Positional(ref mut iter) => iter.next()
        }
    }
}

impl<'a> SeekingIterator for QueryResultIterator<'a> {
    type Item = Posting;

    fn next_seek(&mut self, target: &Self::Item) -> Option<Self::Item> {
        match *self {
            QueryResultIterator::Empty => None,
            QueryResultIterator::Atom(ref mut iter) => iter.next_seek(target),
            QueryResultIterator::NAry(ref mut iter) => iter.next_seek(target),
            QueryResultIterator::Filter(ref mut iter) => iter.next_seek(target),
            QueryResultIterator::Positional(ref mut iter) => iter.next_seek(target),
        }
    }
}

impl<'a> QueryResultIterator<'a> {
    /// Used to be able to sort queries according to their estimated number of
    /// results
    /// This can be used to optimize efficiency on intersecting queries
    fn estimate_length(&self) -> usize {
        match *self {
            QueryResultIterator::Empty => 0,
            QueryResultIterator::Atom(ref iter) => iter.len(),
            QueryResultIterator::NAry(ref iter) => iter.estimate_length(),
            QueryResultIterator::Filter(ref iter) => iter.estimate_length(),
            QueryResultIterator::Positional(ref iter) => iter.estimate_length()
        }
    }

    pub fn peekable_seekable(self) -> PeekableSeekable<Self> {
        PeekableSeekable::new(self)
    }
}

pub struct FilterIterator<'a> {
    operator: FilterOperator,
    sand: Box<PeekableSeekable<QueryResultIterator<'a>>>,
    sieve: Box<PeekableSeekable<QueryResultIterator<'a>>>,
}

impl<'a> Iterator for FilterIterator<'a> {
    type Item = Posting;

    fn next(&mut self) -> Option<Posting> {
        match self.operator {
            FilterOperator::Not => self.next_not(),
        }
    }
}

impl<'a> SeekingIterator for FilterIterator<'a> {
    type Item = Posting;

    // TODO: Write meaningful tests for this implementation
    fn next_seek(&mut self, target: &Self::Item) -> Option<Self::Item> {
        self.sand.peek_seek(target);
        self.next()
    }
}

impl<'a> FilterIterator<'a> {
    pub fn new(operator: FilterOperator,
               sand: Box<PeekableSeekable<QueryResultIterator<'a>>>,
               sieve: Box<PeekableSeekable<QueryResultIterator<'a>>>)
               -> Self {
        FilterIterator {
            operator: operator,
            sand: sand,
            sieve: sieve,
        }
    }


    fn estimate_length(&self) -> usize {
        let sand_len = self.sand.inner().estimate_length();
        let sieve_len = self.sieve.inner().estimate_length();
        if sand_len > sieve_len {
            sand_len - sieve_len
        } else {
            0
        }
    }

    fn next_not(&mut self) -> Option<Posting> {
        loop {
            let sand = try_option!(self.sand.next());
            if let Some(sieve) = self.sieve.peek_seek(&sand) {
                if sieve.0 == sand.0 {
                    continue;
                }
            }
            return Some(sand);
        }
    }
}

#[cfg(test)]
mod tests {

    use index::boolean_index::boolean_query::*;
    use index::boolean_index::tests::prepare_index;


    #[test]
    fn peek() {
        let index = prepare_index();
        let mut qri = index.run_atom(&0).peekable_seekable();
        let doc_id_1 = qri.peek().map(|p| *p.doc_id());
        let doc_id_2 = qri.peek().map(|p| *p.doc_id());
        assert_eq!(doc_id_1, doc_id_2);
        let mut qri2 = index.run_nary_query(&BooleanOperator::And,
                            &vec![BooleanQuery::Atom(QueryAtom::new(0, 0)), BooleanQuery::Atom(QueryAtom::new(0, 0))])
            .peekable_seekable();
        let doc_id_1 = qri2.peek().map(|p| *p.doc_id());
        let doc_id_2 = qri2.peek().map(|p| *p.doc_id());
        assert_eq!(doc_id_1, doc_id_2);
    }

    #[test]
    fn estimate_length() {
        let index = prepare_index();
        assert!(index.run_atom(&0).estimate_length() == 3);
        assert!(index.run_atom(&3).estimate_length() == 2);
        assert!(index.run_atom(&16).estimate_length() == 1);
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
