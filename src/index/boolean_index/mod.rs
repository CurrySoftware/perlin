use index::Index;

use std::collections::BTreeMap;
use std::iter::Iterator;

use std::fmt::{Formatter, Result, Debug};

use index::boolean_index::query_result_iterator::*;
use index::boolean_index::posting::Posting;

mod query_result_iterator;
mod persistence;

mod posting {
    // For each term-document pair the doc_id and the
    // positions of the term inside the document are stored
    pub type Posting = (u64 /* doc_id */, Vec<u32> /* positions */);
}


#[derive(Clone)]
pub enum BooleanOperator {
    Or,
    And,
}

#[derive(Clone)]
pub enum FilterOperator {
    Not,
}

#[derive(Clone)]
pub enum PositionalOperator {
    InOrder,
}

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
    Positional(PositionalOperator, Vec<QueryAtom<TTerm>>),
    NAry(BooleanOperator,
              Vec<BooleanQuery<TTerm>>,
              Option<(FilterOperator, Box<BooleanQuery<TTerm>>)>),
}

pub struct BooleanIndex<TTerm: Ord> {
    document_count: usize,
    index: BTreeMap<TTerm, Vec<Posting>>
}

impl<TTerm: Debug + Ord> Debug for BooleanIndex<TTerm> {
    fn fmt(&self, f: &mut Formatter) -> Result {
        let res = writeln!(f,
                           "Document Count: {} Term Count: {}",
                           self.document_count,
                           self.index.len());
        for (term, postings) in &self.index {
            if let Err(e) = write!(f,
                   "[{:?} df:{} cf:{}]",
                   term,
                   postings.len(),
                   postings.iter()
                       .map(|&(_, ref positions)| positions.len())
                                   .fold(0, |acc, x| acc + x)) {
                return Err(e);
            }
        }
        res
    }
}


impl<'a, TTerm: Ord> Index<'a, TTerm> for BooleanIndex<TTerm> {
    type Query = BooleanQuery<TTerm>;
    type QueryResult = Box<Iterator<Item=u64> + 'a>;

    fn new() -> BooleanIndex<TTerm> {
        BooleanIndex {
            document_count: 0,
            index: BTreeMap::new()
        }
    }

    /// Indexes a document for later retrieval
    /// Returns the document_id used by the index
    fn index_document<TDocIterator: Iterator<Item = TTerm>>(&mut self,
                                                            document: TDocIterator)
                                                            -> u64 {
        let new_doc_id = self.document_count;
        for (term_position, term) in document.enumerate() {
            // Get all doc_ids in BTree for a term
            if let Some(listing) = self.index.get_mut(&term) {
                // check if document is already there
                match listing.binary_search_by(|&(doc_id, _)| doc_id.cmp(&(new_doc_id as u64))) {
                    Ok(term_doc_index) => {
                        // Document already had that term.
                        // Look for where to put the current term in the positions list
                        let term_doc_positions = &mut listing.get_mut(term_doc_index).unwrap().1;
                        if let Err(index) =
                               term_doc_positions.binary_search(&(term_position as u32)) {
                                   // Two terms at the same position. Should at least be possible
                                   //so do nothing if term_position already exists
                            term_doc_positions.insert(index, term_position as u32)
                        }
                    }
                    Err(term_doc_index) => {
                        listing.insert(term_doc_index,
                                       (new_doc_id as u64, vec![term_position as u32]))
                    }

                }
                // Term is indexed. Continue with the next one
                continue;
            };
            // Term was not in BTree. Add it
            self.index.insert(term, vec![(new_doc_id as u64, vec![term_position as u32])]);
        }
        self.document_count += 1;
        new_doc_id as u64
    }

    fn execute_query(&'a self, query: &Self::Query) -> Self::QueryResult {
        match self.run_query(query) {
            QueryResultIterator::Empty => Box::new(Vec::<u64>::new().into_iter()),
            QueryResultIterator::Atom(_, iter) => {
               Box::new(iter.map(|&(doc_id, _)| doc_id))
            }
            QueryResultIterator::NAry(iter) => {
               Box::new(iter.map(|&(doc_id, _)| doc_id))
            }
        }
    }
}


impl<TTerm: Ord> BooleanIndex<TTerm> {
    fn run_query(&self, query: &BooleanQuery<TTerm>) -> QueryResultIterator {
        match *query {
            BooleanQuery::Atom(ref atom) => {
                self.run_atom(atom.relative_position, &atom.query_term)
            }
            BooleanQuery::NAry(ref operator, ref operands, ref filter) => {
                self.run_nary_query(operator, operands, filter)
            }
            BooleanQuery::Positional(ref operator, ref operands) => {
                self.run_positional_query(operator, operands)
            }
        }

    }

    fn run_nary_query(&self,
                      operator: &BooleanOperator,
                      operands: &[BooleanQuery<TTerm>],
                      filter: &Option<(FilterOperator, Box<BooleanQuery<TTerm>>)>)
                      -> QueryResultIterator {

        let new_filter = if let Some((ref operator, ref operand)) = *filter {
            Some((operator.clone(), Box::new(self.run_query(operand))))
        } else {
            None
        };
        QueryResultIterator::NAry(NAryQueryIterator::new(operator.clone(),
                                                         operands.iter()
                                                             .map(|op| self.run_query(op))
                                                             .collect::<Vec<_>>(),
                                                         new_filter))
    }

    fn run_positional_query(&self,
                            operator: &PositionalOperator,
                            operands: &[QueryAtom<TTerm>])
                            -> QueryResultIterator {
        QueryResultIterator::NAry(NAryQueryIterator::new_positional(operator.clone(),
                                                                    operands.into_iter()
                                                                        .map(|op| {
                                                                            self.run_atom(
                                                      op.relative_position,
                                                      &op.query_term)
                                                                        })
                                                                        .collect::<Vec<_>>()))
    }


    fn run_atom(&self, relative_position: usize, atom: &TTerm) -> QueryResultIterator {
        if let Some(result) = self.index.get(atom) {
            QueryResultIterator::Atom(relative_position, result.iter().peekable())
        } else {
            QueryResultIterator::Empty
        }
    }
}




// --- Tests

#[cfg(test)]
mod tests {
    use super::*;
    use index::Index;

    pub fn prepare_index() -> BooleanIndex<usize> {
        let mut index = BooleanIndex::new();
        index.index_document(0..10);
        index.index_document((0..10).map(|i| i * 2));
        index.index_document(vec![5, 4, 3, 2, 1, 0].into_iter());

        index
    }



    #[test]
    fn empty_query() {
        let index = prepare_index();
        assert!(index.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 15))).collect::<Vec<_>>() ==
                vec![]);

    }



    #[test]
    fn indexing() {
        let index = prepare_index();
        // Check number of docs
        assert!(index.document_count == 3);
        // Check number of terms (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 14, 16, 18)
        assert!(index.index.len() == 15);
        assert!(*index.index.get(&0).unwrap() == vec![(0, vec![0]), (1, vec![0]), (2, vec![5])]);
    }

    #[test]
    fn query_atom() {
        let index = prepare_index();

        assert!(index.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 7))).collect::<Vec<_>>() ==
                vec![0]);
        assert!(index.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 5))).collect::<Vec<_>>() ==
                vec![0, 2]);
        assert!(index.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 0))).collect::<Vec<_>>() ==
                vec![0, 1, 2]);
        assert!(index.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 16))).collect::<Vec<_>>() ==
                vec![1]);
    }

    #[test]
    fn nary_query() {
        let index = prepare_index();

        assert!(index.execute_query(&BooleanQuery::NAry(BooleanOperator::And,
                                                    vec![BooleanQuery::Atom(QueryAtom::new(0,
                                                                                           5)),
                                                         BooleanQuery::Atom(QueryAtom::new(0,
                                                                                           0))],
                                                    None))
            .collect::<Vec<_>>() ==
                vec![0, 2]);
        assert!(index.execute_query(&BooleanQuery::NAry(BooleanOperator::And,
                                                    vec![BooleanQuery::Atom(QueryAtom::new(0,
                                                                                           0)),
                                                         BooleanQuery::Atom(QueryAtom::new(0,
                                                                                           5))],
                                                    None))
            .collect::<Vec<_>>() ==
                vec![0, 2]);
    }

    #[test]
    fn and_query() {
        let index = prepare_index();
        assert!(index.execute_query(&BooleanQuery::NAry(BooleanOperator::And,
                                                    vec![BooleanQuery::Atom(QueryAtom::new(0, 3)),
                                                         BooleanQuery::Atom(QueryAtom::new(0, 12))],
                                                    None))
            .collect::<Vec<_>>() == vec![]);
        assert!(index.execute_query(&BooleanQuery::NAry(BooleanOperator::And,
                                                    vec![BooleanQuery::Atom(QueryAtom::new(0, 14)),
                                                         BooleanQuery::Atom(QueryAtom::new(0, 12))],
                                                    None))
            .collect::<Vec<_>>() == vec![1]);
        assert!(index.execute_query(
            &BooleanQuery::NAry(
                BooleanOperator::And,
                vec![BooleanQuery::NAry(BooleanOperator::And,
                    vec![BooleanQuery::Atom(QueryAtom::new(0, 3)),
                        BooleanQuery::Atom(QueryAtom::new(0, 9))],
                    None),
                    BooleanQuery::Atom(QueryAtom::new(0, 12))],
                None))
                .collect::<Vec<_>>() == vec![]);
        assert!(index.execute_query(
            &BooleanQuery::NAry(
                BooleanOperator::And,
                vec![BooleanQuery::NAry(BooleanOperator::And,
                    vec![BooleanQuery::Atom(QueryAtom::new(0, 2)),
                        BooleanQuery::Atom(QueryAtom::new(0, 4))],
                    None),
                    BooleanQuery::Atom(QueryAtom::new(0, 16))],
                None))
                .collect::<Vec<_>>() == vec![1]);
    }

    #[test]
    fn or_query() {
        let index = prepare_index();
        assert!(index.execute_query(&BooleanQuery::NAry(BooleanOperator::Or,
                                                    vec![BooleanQuery::Atom(QueryAtom::new(0, 3)),
                                                         BooleanQuery::Atom(QueryAtom::new(0, 12))],
                                                    None))
            .collect::<Vec<_>>() == vec![0, 1, 2]);
        assert!(index.execute_query(&BooleanQuery::NAry(BooleanOperator::Or,
                                                    vec![BooleanQuery::Atom(QueryAtom::new(0, 14)),
                                                         BooleanQuery::Atom(QueryAtom::new(0, 12))],
                                                    None))
            .collect::<Vec<_>>() == vec![1]);
        assert!(index.execute_query(
            &BooleanQuery::NAry(
                BooleanOperator::Or,
                vec![BooleanQuery::NAry(BooleanOperator::Or,
                    vec![BooleanQuery::Atom(QueryAtom::new(0, 3)),
                        BooleanQuery::Atom(QueryAtom::new(0, 9))],
                    None),
                    BooleanQuery::Atom(QueryAtom::new(0, 16))],
                None))
                .collect::<Vec<_>>() == vec![0,1,2]);
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
            &BooleanQuery::NAry(
                BooleanOperator::And,
                vec![BooleanQuery::Atom(QueryAtom::new(0, 2)),
                     BooleanQuery::Atom(QueryAtom::new(0, 0))],
                Some((FilterOperator::Not,
                      Box::new(BooleanQuery::Atom(
                          QueryAtom::new(0, 16))))))).collect::<Vec<_>>() == vec![0,2]);



    }

}
