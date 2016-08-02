use index::Index;

use std::collections::BTreeMap;
use std::iter::Iterator;
use std::sync::Arc;
use std::cell::Cell;

use index::storage::Storage;
use index::boolean_index::query_result_iterator::*;
use index::boolean_index::query_result_iterator::nary_query_iterator::*;
use index::boolean_index::posting::Posting;

mod query_result_iterator;

// TODO: Remove Pub. WRONG!.
// Remove as soon as vbyte_encode and VByteDecoder are abstracted away from perlin or at least boolean index
pub mod persistence;


// not intended for public use. Thus the wrapper module
// TODO: REMOVE PUB. WRONG!.
// Remove as soon as Posting is abstraced from fs_storage
pub mod posting {
    // For each term-document pair the doc_id and the
    // positions of the term inside the document are stored
    pub type Posting = (u64 /* doc_id */, Vec<u32> /* positions */);
}

/// Basic boolean operator. Use it in combination with a `BooleanQuery`
#[derive(Copy ,Clone)]
pub enum BooleanOperator {
    Or,
    And,
}

/// Basic filter operator. Use it in combination with a `BooleanQuery`
#[derive(Copy, Clone)]
pub enum FilterOperator {
    Not,
}

/// Basic positional operator. Use it in combination with a `BooleanQuery`
#[derive(Copy, Clone)]
pub enum PositionalOperator {
    /// Ensures that QueryAtoms are in the specified order and placement
    /// See `BooleanQuery::Positional` for more information
    InOrder,
}

/// Stores term to be compared against and relative position of a query atom
pub struct QueryAtom<TTerm> {
    relative_position: usize,
    query_term: TTerm,
}

impl<TTerm> QueryAtom<TTerm> {
    pub fn new(relative_position: usize, query_term: TTerm) -> Self {
        QueryAtom {
            relative_position: relative_position,
            query_term: query_term,
        }
    }
}


pub enum BooleanQuery<TTerm> {
    Atom(QueryAtom<TTerm>),
    // Different from NAry because positional queries can currently only run on query-atoms.
    // To ensure correct usage, this rather inelegant abstraction was implemented
    // Nevertheless, internally both are handled by the same code
    // See `NAryQueryIterator::new` and `NAryQueryIterator::new_positional`
    Positional(PositionalOperator, Vec<QueryAtom<TTerm>>),
    NAry(BooleanOperator, Vec<BooleanQuery<TTerm>>),
    Filter(FilterOperator,
           // sand
           Box<BooleanQuery<TTerm>>,
           // sieve
           Box<BooleanQuery<TTerm>>),
}

pub struct BooleanIndex<TTerm: Ord> {
    document_count: usize,
    term_ids: BTreeMap<TTerm, u64>,
    postings: Box<Storage<Vec<Posting>>>,
}

impl<'a, TTerm: Ord> Index<'a, TTerm> for BooleanIndex<TTerm> {
    type Query = BooleanQuery<TTerm>;
    type QueryResult = Box<Iterator<Item = u64> + 'a>;

    /// Indexes a document collection for later retrieval
    /// Returns the document_ids used by the index
    // First Shot
    fn index_documents<TDocIterator: Iterator<Item = TTerm>>(&mut self,
                                                             documents: Vec<TDocIterator>)
                                                             -> Vec<u64> {
        let mut inv_index: BTreeMap<u64, Vec<Posting>> = BTreeMap::new();
        let mut result = Vec::with_capacity(documents.len());
        // For every document in the collection
        for document in documents {
            // Determine its id. consecutively numbered
            let new_doc_id = self.document_count as u64;
            // Enumerate over its terms
            for (term_position, term) in document.enumerate() {
                // Has term already been seen? Is it already in the vocabulary?
                if let Some(term_id) = self.term_ids.get(&term) {
                    // Get its listing from the temporary. And add doc_id and/or position to it
                    let listing = inv_index.get_mut(term_id).unwrap();
                    match listing.binary_search_by(|&(doc_id, _)| doc_id.cmp(&new_doc_id)) {
                        Ok(term_doc_index) => {
                            // Document already had that term.
                            // Look for where to put the current term in the positions list
                            let term_doc_positions =
                                &mut listing.get_mut(term_doc_index).unwrap().1;
                            if let Err(index) =
                                   term_doc_positions.binary_search(&(term_position as u32)) {
                                term_doc_positions.insert(index, term_position as u32)
                            }
                            // Two terms at the same position. Should at least be possible
                            // so do nothing if term_position already exists
                        }
                        Err(term_doc_index) => {
                            listing.insert(term_doc_index,
                                           (new_doc_id as u64, vec![term_position as u32]))
                        }
                    }
                    // Term is indexed. Continue with the next one
                    continue;
                };
                // Term was not yet indexed. Add it
                let term_id = self.term_ids.len() as u64;
                self.term_ids.insert(term, term_id);
                inv_index.insert(term_id, vec![(new_doc_id, vec![term_position as u32])]);
            }
            self.document_count += 1;
            result.push(new_doc_id);
        }

        // everything is now indexed. Hand it to our provider.
        // We do not care where it saves our data.
        for (term_id, listing) in inv_index {
            self.postings.store(term_id, listing);
        }

        result
    }



    fn execute_query(&'a self, query: &Self::Query) -> Self::QueryResult {
        match self.run_query(query) {
            QueryResultIterator::Empty => Box::new(Vec::<u64>::new().into_iter()),
            QueryResultIterator::Atom(_, iter) => {
                let mut res = Vec::with_capacity(iter.len());
                for _ in 0..iter.len() {
                    res.push(iter.next().unwrap().0)
                }
                Box::new(res.into_iter())
            }
            QueryResultIterator::NAry(iter) => {
                let mut res = Vec::new();
                while let Some(posting) = iter.next() {
                    res.push(posting.0)
                }
                Box::new(res.into_iter())
            }
            QueryResultIterator::Filter(iter) => {
                let mut res = Vec::new();
                while let Some(posting) = iter.next() {
                    res.push(posting.0)
                }
                Box::new(res.into_iter())
            }
        }
    }
}


impl<TTerm: Ord> BooleanIndex<TTerm> {
    pub fn new(provider: Box<Storage<Vec<Posting>>>) -> BooleanIndex<TTerm> {
        BooleanIndex {
            document_count: 0,
            term_ids: BTreeMap::new(),
            postings: provider,
        }
    }


    fn run_query(&self, query: &BooleanQuery<TTerm>) -> QueryResultIterator {
        match *query {
            BooleanQuery::Atom(ref atom) => self.run_atom(atom.relative_position, &atom.query_term),
            BooleanQuery::NAry(ref operator, ref operands) => {
                self.run_nary_query(operator, operands)
            }
            BooleanQuery::Positional(ref operator, ref operands) => {
                self.run_positional_query(operator, operands)
            }
            BooleanQuery::Filter(ref operator, ref sand, ref sieve) => {
                self.run_filter(operator, sand.as_ref(), sieve.as_ref())
            }

        }

    }

    fn run_nary_query(&self,
                      operator: &BooleanOperator,
                      operands: &[BooleanQuery<TTerm>])
                      -> QueryResultIterator {
        let mut ops = Vec::new();
        for operand in operands {
            ops.push(self.run_query(operand))
        }
        QueryResultIterator::NAry(NAryQueryIterator::new(*operator, ops))
    }

    fn run_positional_query(&self,
                            operator: &PositionalOperator,
                            operands: &[QueryAtom<TTerm>])
                            -> QueryResultIterator {
        let mut ops = Vec::new();
        for operand in operands {
            ops.push(self.run_atom(operand.relative_position, &operand.query_term));
        }
        QueryResultIterator::NAry(NAryQueryIterator::new_positional(*operator, ops))
    }

    fn run_filter(&self,
                  operator: &FilterOperator,
                  sand: &BooleanQuery<TTerm>,
                  sieve: &BooleanQuery<TTerm>)
                  -> QueryResultIterator {
        QueryResultIterator::Filter(FilterIterator::new(*operator,
                                                        Box::new(self.run_query(sand)),
                                                        Box::new(self.run_query(sieve))))
    }


    fn run_atom(&self, relative_position: usize, atom: &TTerm) -> QueryResultIterator {
        if let Some(result) = self.term_ids.get(atom) {
            QueryResultIterator::Atom(relative_position,
                                      ArcIter {
                                          data: self.postings.get(*result).unwrap(),
                                          pos: Cell::new(0),
                                      })
        } else {
            QueryResultIterator::Empty
        }
    }
}


pub trait OwningIterator<'a> {
    type Item;
    fn next(&'a self) -> Option<Self::Item>;
    fn peek(&'a self) -> Option<Self::Item>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;
}

pub struct ArcIter<T> {
    data: Arc<Vec<T>>,
    pos: Cell<usize>,
}

impl<'a, T: 'a> OwningIterator<'a> for ArcIter<T> {
    type Item = &'a T;

    fn next(&'a self) -> Option<Self::Item> {
        if self.pos.get() < self.data.len() {
            self.pos.set(self.pos.get() + 1);
            return Some(&self.data[self.pos.get() - 1]);
        }
        None
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn peek(&'a self) -> Option<Self::Item> {
        if self.pos.get() >= self.len() {
            None
        } else {
            Some(&self.data[self.pos.get()])
        }
    }

    fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}


// --- Tests

#[cfg(test)]
mod tests {
    use super::*;
    use index::Index;
    use index::storage::ram_storage::RamStorage;


    pub fn prepare_index() -> BooleanIndex<usize> {
        let mut index = BooleanIndex::new(Box::new(RamStorage::new()));
        index.index_documents(vec![(0..10).collect::<Vec<_>>().into_iter(),
                                   (0..10).map(|i| i * 2).collect::<Vec<_>>().into_iter(),
                                   vec![5, 4, 3, 2, 1, 0].into_iter()]);
        index
    }


    #[test]
    fn empty_query() {
        let index = prepare_index();
        assert!(index.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 15)))
            .collect::<Vec<_>>() == vec![]);

    }



    #[test]
    fn indexing() {
        let index = prepare_index();
        // Check number of docs
        assert!(index.document_count == 3);
        // Check number of terms (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 14, 16, 18)
        assert!(index.term_ids.len() == 15);
        assert!(*index.postings.get(*index.term_ids.get(&0).unwrap()).unwrap() ==
                vec![(0, vec![0]), (1, vec![0]), (2, vec![5])]);
    }

    #[test]
    fn query_atom() {
        let index = prepare_index();

        assert!(index.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 7)))
            .collect::<Vec<_>>() == vec![0]);
        assert!(index.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 5)))
            .collect::<Vec<_>>() == vec![0, 2]);
        assert!(index.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 0)))
            .collect::<Vec<_>>() == vec![0, 1, 2]);
        assert!(index.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 16)))
            .collect::<Vec<_>>() == vec![1]);
    }

    #[test]
    fn nary_query() {
        let index = prepare_index();

        assert!(index.execute_query(&BooleanQuery::NAry(BooleanOperator::And,
                                               vec![BooleanQuery::Atom(QueryAtom::new(0, 5)),
                                                    BooleanQuery::Atom(QueryAtom::new(0, 0))]))
            .collect::<Vec<_>>() == vec![0, 2]);
        assert!(index.execute_query(&BooleanQuery::NAry(BooleanOperator::And,
                                               vec![BooleanQuery::Atom(QueryAtom::new(0, 0)),
                                                    BooleanQuery::Atom(QueryAtom::new(0, 5))]))
            .collect::<Vec<_>>() == vec![0, 2]);
    }

    #[test]
    fn and_query() {
        let index = prepare_index();
        assert!(index.execute_query(&BooleanQuery::NAry(BooleanOperator::And,
                                               vec![BooleanQuery::Atom(QueryAtom::new(0, 3)),
                                                    BooleanQuery::Atom(QueryAtom::new(0,
                                                                                      12))]))
            .collect::<Vec<_>>() == vec![]);
        assert!(index.execute_query(&BooleanQuery::NAry(BooleanOperator::And,
                                               vec![BooleanQuery::Atom(QueryAtom::new(0,
                                                                                      14)),
                                                    BooleanQuery::Atom(QueryAtom::new(0,
                                                                                      12))]))
            .collect::<Vec<_>>() == vec![1]);
        assert!(index.execute_query(&BooleanQuery::NAry(BooleanOperator::And,
                                               vec![BooleanQuery::NAry(BooleanOperator::And,
                    vec![BooleanQuery::Atom(QueryAtom::new(0, 3)),
                        BooleanQuery::Atom(QueryAtom::new(0, 9))]
                    ),
                    BooleanQuery::Atom(QueryAtom::new(0, 12))]))
            .collect::<Vec<_>>() == vec![]);
        assert!(index.execute_query(&BooleanQuery::NAry(BooleanOperator::And,
                                               vec![BooleanQuery::NAry(BooleanOperator::And,
                    vec![BooleanQuery::Atom(QueryAtom::new(0, 2)),
                        BooleanQuery::Atom(QueryAtom::new(0, 4))]
                    ),
                    BooleanQuery::Atom(QueryAtom::new(0, 16))]))
            .collect::<Vec<_>>() == vec![1]);
    }

    #[test]
    fn or_query() {
        let index = prepare_index();
        assert_eq!(index.execute_query(&BooleanQuery::NAry(BooleanOperator::Or,
                                               vec![BooleanQuery::Atom(QueryAtom::new(0, 3)),
                                                    BooleanQuery::Atom(QueryAtom::new(0,
                                                                                      12))]))
            .collect::<Vec<_>>(), vec![0, 1, 2]);
        assert_eq!(index.execute_query(&BooleanQuery::NAry(BooleanOperator::Or,
                                                          vec![BooleanQuery::Atom(QueryAtom::new(0,
                                                                                      14)),
                                                    BooleanQuery::Atom(QueryAtom::new(0,
                                                                                      12))]))
                       .collect::<Vec<_>>(),
                   vec![1]);
        assert_eq!(index.execute_query(&BooleanQuery::NAry(BooleanOperator::Or,
                                               vec![BooleanQuery::NAry(BooleanOperator::Or,
                    vec![BooleanQuery::Atom(QueryAtom::new(0, 3)),
                        BooleanQuery::Atom(QueryAtom::new(0, 9))]
                    ),
                    BooleanQuery::Atom(QueryAtom::new(0, 16))]))
            .collect::<Vec<_>>(), vec![0, 1, 2]);
    }

    #[test]
    fn inorder_query() {
        let index = prepare_index();
        assert!(index.execute_query(&BooleanQuery::Positional(PositionalOperator::InOrder,
                                                     vec![QueryAtom::new(0, 0),
                                                          QueryAtom::new(1, 1)]))
            .collect::<Vec<_>>() == vec![0]);
        assert!(index.execute_query(&BooleanQuery::Positional(PositionalOperator::InOrder,
                                                     vec![QueryAtom::new(1, 0),
                                                          QueryAtom::new(0, 1)]))
            .collect::<Vec<_>>() == vec![2]);
        assert!(index.execute_query(&BooleanQuery::Positional(PositionalOperator::InOrder,
                                                     vec![QueryAtom::new(0, 0),
                                                          QueryAtom::new(1, 2)]))
            .collect::<Vec<_>>() == vec![1]);

        assert!(index.execute_query(&BooleanQuery::Positional(PositionalOperator::InOrder,
                                                     vec![QueryAtom::new(2, 2),
                                                          QueryAtom::new(1, 1),
                                                          QueryAtom::new(0, 0)]))
            .collect::<Vec<_>>() == vec![0]);
        assert!(index.execute_query(&BooleanQuery::Positional(PositionalOperator::InOrder,
                                                     vec![QueryAtom::new(0, 2),
                                                          QueryAtom::new(1, 1),
                                                          QueryAtom::new(2, 0)]))
            .collect::<Vec<_>>() == vec![2]);
        assert!(index.execute_query(&BooleanQuery::Positional(PositionalOperator::InOrder,
                                                     vec![QueryAtom::new(0, 2),
                                                          QueryAtom::new(1, 1),
                                                          QueryAtom::new(3, 0)]))
            .collect::<Vec<_>>() == vec![]);
    }

    #[test]
    fn query_filter() {
        let index = prepare_index();
        assert!(index.execute_query(
            &BooleanQuery::Filter(FilterOperator::Not,
            Box::new(BooleanQuery::NAry(
                BooleanOperator::And,
                vec![BooleanQuery::Atom(QueryAtom::new(0, 2)),
                     BooleanQuery::Atom(QueryAtom::new(0, 0))])),
                      Box::new(BooleanQuery::Atom(
                          QueryAtom::new(0, 16))))).collect::<Vec<_>>() == vec![0,2]);



    }

}
