use index::Index;

use std::collections::BTreeMap;
use std::slice::Iter;
use std::iter::Iterator;
use std::iter::Peekable;

// For each term-document pair the doc_id and the
// positions of the term inside the document are stored
type Posting = (usize /* doc_id */, Vec<usize> /* positions */);

macro_rules! unwrap_or_return_none{
    ($operand:expr) => {
        if let Some(x) = $operand {
            x
        } else {
            return None;
        }
    }
}

#[derive(Debug)]
pub struct BooleanIndex<TTerm: Ord> {
    document_count: usize,
    index: BTreeMap<TTerm, Vec<Posting>>,
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
    PositionalQuery(PositionalOperator, Vec<QueryAtom<TTerm>>),
    NAryQuery(BooleanOperator,
              Vec<BooleanQuery<TTerm>>,
              Option<(FilterOperator, Box<BooleanQuery<TTerm>>)>),
}

pub struct BooleanQueryResult {
    document_ids: Vec<usize>,
}

impl<TTerm: Ord> Index<TTerm> for BooleanIndex<TTerm> {
    type Query = BooleanQuery<TTerm>;
    type QueryResult = BooleanQueryResult;

    fn new() -> BooleanIndex<TTerm> {
        BooleanIndex {
            document_count: 0,
            index: BTreeMap::new(),
        }
    }

    /// Indexes a document for later retrieval
    /// Returns the document_id used by the index
    fn index_document<TDocIterator: Iterator<Item = TTerm>>(&mut self,
                                                            document: TDocIterator)
                                                            -> usize {
        let new_doc_id = self.document_count;
        for (term_position, term) in document.enumerate() {
            // Get all doc_ids in BTree for a term
            if let Some(listing) = self.index.get_mut(&term) {
                // check if document is already there
                match listing.binary_search_by(|&(doc_id, _)| doc_id.cmp(&new_doc_id)) {
                    Ok(term_doc_index) => {
                        // Document already had that term.
                        // Look for where to put the current term in the positions list
                        let ref mut term_doc_positions = listing.get_mut(term_doc_index).unwrap().1;
                        match term_doc_positions.binary_search(&term_position) {
                            Err(index) => term_doc_positions.insert(index, term_position),
                            Ok(_) => {}
                            // Two terms at the same position. Should at least be possible
                            // so: Do nothing
                        }
                    }
                    Err(term_doc_index) => {
                        listing.insert(term_doc_index, (new_doc_id, vec![term_position]))
                    }

                }
                // Term is indexed. Continue with the next one
                continue;
            };
            // Term was not in BTree. Add it
            self.index.insert(term, vec![(new_doc_id, vec![term_position])]);
        }
        self.document_count += 1;
        new_doc_id
    }

    fn execute_query(&self, query: &Self::Query) -> Self::QueryResult {
        match self.run_query(query) {
            QueryResultIterator::EmptyQuery => {
                BooleanQueryResult{
                    document_ids: vec![]
                }
            }
            QueryResultIterator::AtomQuery(_, iter) => {
                BooleanQueryResult {
                    document_ids: iter.map(|&(doc_id, _)| doc_id).collect::<Vec<_>>(),
                }
            }
            QueryResultIterator::NAryQuery(iter) => {
                BooleanQueryResult {
                    document_ids: iter.map(|&(doc_id, _)| doc_id).collect::<Vec<_>>(),
                }
            }
        }
    }
}


impl<TTerm: Ord> BooleanIndex<TTerm> {
    fn run_query(&self, query: &BooleanQuery<TTerm>) -> QueryResultIterator {
        match query {
            &BooleanQuery::Atom(ref atom) => {
                self.run_atom(atom.relative_position, &atom.query_term)
            }
            &BooleanQuery::NAryQuery(ref operator, ref operands, ref filter) => {
                self.run_nary_query(operator, operands, filter)
            }
            &BooleanQuery::PositionalQuery(ref operator, ref operands) => {
                self.run_positional_query(operator, operands)
            }
        }

    }

    fn run_nary_query(&self,
                      operator: &BooleanOperator,
                      operands: &Vec<BooleanQuery<TTerm>>,
                      filter: &Option<(FilterOperator, Box<BooleanQuery<TTerm>>)>)
                      -> QueryResultIterator {

        let new_filter = if let &Some((ref operator, ref operand)) = filter {
            Some((operator.clone(), Box::new(self.run_query(&operand))))
        } else {
            None
        };
        QueryResultIterator::NAryQuery(NAryQueryIterator::new(operator.clone(),
                                                              operands.iter()
                                                                  .map(|op| self.run_query(op))
                                                                  .collect::<Vec<_>>(),
                                                              new_filter))
    }

    fn run_positional_query(&self,
                            operator: &PositionalOperator,
                            operands: &Vec<QueryAtom<TTerm>>)
                            -> QueryResultIterator {
        QueryResultIterator::NAryQuery(NAryQueryIterator::new_positional(operator.clone(),
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
        QueryResultIterator::AtomQuery(relative_position,
                                       result.iter().peekable())
        } else {
            QueryResultIterator::EmptyQuery
        }
    }
}

enum QueryResultIterator<'a> {
    EmptyQuery,
    AtomQuery(usize, Peekable<Iter<'a, Posting>>),
    NAryQuery(NAryQueryIterator<'a>),
}

impl<'a> QueryResultIterator<'a> {
    fn estimate_length(&self) -> usize {
        match self {
            &QueryResultIterator::EmptyQuery => 0,
            &QueryResultIterator::AtomQuery(_, ref iter) => iter.len(),
            &QueryResultIterator::NAryQuery(ref iter) => iter.estimate_length(),

        }
    }

    fn relative_position(&self) -> usize {
        match self {
            &QueryResultIterator::EmptyQuery => 0,
            &QueryResultIterator::AtomQuery(rpos, _) => rpos,
            &QueryResultIterator::NAryQuery(_) => 0,
        }

    }

    fn next(&mut self) -> Option<&'a Posting> {
        match self {
            &mut QueryResultIterator::EmptyQuery => None,
            &mut QueryResultIterator::AtomQuery(_, ref mut iter) => iter.next(),
            &mut QueryResultIterator::NAryQuery(ref mut iter) => iter.next(),
        }
    }

    fn peek(&mut self) -> Option<&'a Posting> {
        match self {
            &mut QueryResultIterator::EmptyQuery => None,
            &mut QueryResultIterator::AtomQuery(_, ref mut iter) => iter.peek().map(|val| *val),
            &mut QueryResultIterator::NAryQuery(ref mut iter) => iter.peek(),
        }
    }
}

struct NAryQueryIterator<'a> {
    pos_operator: Option<PositionalOperator>,
    bool_operator: Option<BooleanOperator>,
    operands: Vec<QueryResultIterator<'a>>,
    filter: Option<(FilterOperator, Box<QueryResultIterator<'a>>)>,
    peeked_value: Option<Option<&'a Posting>>,
}

impl<'a> Iterator for NAryQueryIterator<'a> {
    type Item = &'a Posting;
    fn next(&mut self) -> Option<&'a Posting> {
        if self.filter.is_some() {
            return self.filtered_next();
        }
        if let Some(next) = self.peeked_value {
            self.peeked_value = None;
            return next;
        }
        match self.bool_operator {
            Some(BooleanOperator::And) => self.next_and(),
            Some(BooleanOperator::Or) => self.next_or(),
            None => {
                match self.pos_operator {
                    Some(PositionalOperator::InOrder) => self.next_inorder(),
                    None => {
                        assert!(false);
                        None
                    }
                }
            }
        }
    }
}

impl<'a> NAryQueryIterator<'a> {
    fn new_positional(operator: PositionalOperator,
                      operands: Vec<QueryResultIterator<'a>>)
                      -> NAryQueryIterator<'a> {
        let mut result = NAryQueryIterator {
            pos_operator: Some(operator),
            bool_operator: None,
            operands: operands,
            peeked_value: None,
            filter: None,
        };
        result.operands.sort_by_key(|op| op.estimate_length());
        result
    }


    fn new(operator: BooleanOperator,
           operands: Vec<QueryResultIterator<'a>>,
           filter: Option<(FilterOperator, Box<QueryResultIterator<'a>>)>)
           -> NAryQueryIterator<'a> {
        let mut result = NAryQueryIterator {
            pos_operator: None,
            bool_operator: Some(operator),
            operands: operands,
            peeked_value: None,
            filter: filter,
        };
        result.operands.sort_by_key(|op| op.estimate_length());
        result
    }

    fn filtered_next(&mut self) -> Option<&'a Posting> {
        loop {
            let next = match self.peeked_value {
                Some(n) => {
                    self.peeked_value = None;
                    n
                }
                None => {
                    match self.bool_operator {
                        Some(BooleanOperator::And) => self.next_and(),
                        Some(BooleanOperator::Or) => self.next_or(),
                        None => {
                            match self.pos_operator {
                                Some(PositionalOperator::InOrder) => self.next_inorder(),
                                None => {
                                    assert!(false);
                                    None
                                }
                            }
                        }
                    }
                }
            };
            if let Some(next_posting) = next {                
                if self.filter_check(next_posting) {
                    return next;
                }
            } else {
                return None;
            }
        }
    }

    fn filter_check(&mut self, input: &Posting) -> bool {
        match self.filter {
            Some((FilterOperator::Not, _)) => self.filter_not(input),
            None => {
                unreachable!();
            }
        }

    }

    fn filter_not(&mut self, input: &Posting) -> bool {
        if let Some((_, ref mut boxed_operator)) = self.filter {
            let operator = boxed_operator.as_mut();
            loop {
                if let Some(v) = operator.peek() {
                    if v.0 > input.0 {
                        // Input is smaller than next filtervalue -> let it through
                        return true;
                    } else if v.0 == input.0 {
                        operator.next();
                        return false;
                    } else {
                        operator.next();
                    }
                } else {
                    return true;
                }
            }
        } else {
            unreachable!();
        }
    }

    fn peek(&mut self) -> Option<&'a Posting> {
        if self.peeked_value.is_none() {
            self.peeked_value = Some(match self.bool_operator {
                Some(BooleanOperator::And) => self.next_and(),
                Some(BooleanOperator::Or) => self.next_or(),
                None => {
                    match self.pos_operator {
                        Some(PositionalOperator::InOrder) => self.next_inorder(),
                        None => {
                            assert!(false);
                            None
                        }
                    }
                }
            })
        }
        self.peeked_value.unwrap()
    }

    fn next_inorder(&mut self) -> Option<&'a Posting> {
        let mut focus = None; //Acts as temporary to be compared against
        let mut focus_positions = vec![];
        // The iterator index that last set 'focus'
        let mut last_doc_iter = self.operands.len() + 1;
        // The the relative position of last_doc_iter
        let mut last_positions_iter = self.operands.len() + 1;
        'possible_documents: loop {
            // For every term
            for (i, input) in self.operands.iter_mut().enumerate() {
                'term_documents: loop {
                    // If the focus was set by the current iterator, we have a match
                    if last_doc_iter == i {
                        break 'term_documents;
                    }
                    // Get the next doc_id for the current iterator
                    let mut v = unwrap_or_return_none!(input.next());
                    if focus.is_none() {
                        focus = Some(v);
                        focus_positions = v.1.clone();
                        last_doc_iter = i;
                        last_positions_iter = input.relative_position();
                        break 'term_documents;
                    } else if v.0 < focus.unwrap().0 {
                        // If the doc_id is smaller, get its next value
                        continue 'term_documents;
                    } else if v.0 == focus.unwrap().0 {
                        // If the doc_id is equal, check positions
                        let position_offset = last_positions_iter as i64 -
                                              input.relative_position() as i64;
                        focus_positions = positional_intersect(&focus_positions,
                                                               &v.1,
                                                               (position_offset, position_offset))
                            .iter()
                            .map(|pos| pos.1)
                            .collect::<Vec<_>>();
                        if focus_positions.is_empty() {
                            // No suitable positions found. Next document
                            v = unwrap_or_return_none!(input.next());
                            focus = Some(v);
                            focus_positions = v.1.clone();
                            last_doc_iter = i;
                            last_positions_iter = input.relative_position();
                            continue 'possible_documents;
                        } else {
                            last_positions_iter = input.relative_position();
                            break 'term_documents;
                        }
                    } else {
                        // If it is larger, we are now looking at a different focus.
                        // Reset focus and last_iter. Then start from the beginning
                        focus = Some(v);
                        focus_positions = v.1.clone();
                        last_doc_iter = i;
                        last_positions_iter = input.relative_position();
                        continue 'possible_documents;
                    }
                }
            }
            return focus;
        }
    }

    fn next_or(&mut self) -> Option<&'a Posting> {

        // Find the smallest current value of all operands
        let min_value = self.operands
            .iter_mut()
            .map(|op| op.peek())
            .filter(|val| val.is_some())
            .map(|val| val.unwrap().0)
            .min();

        // If there is such a value
        if let Some(min) = min_value {
            let mut tmp = None;
            let mut i = 0;
            // Loop over all operands. Advance the ones which currently yield that minimal value
            // Throw the ones out which are empty. Then return the minimal value as reference
            while i < self.operands.len() {
                println!("i: {:?}", i);
                if let Some(val) = self.operands[i].peek() {
                    if val.0 == min {
                        tmp = self.operands[i].next();
                    }
                    i += 1;
                } else {
                    // Operand does not yield any more results. Kick it out.
                    self.operands.remove(i);
                }
            }
            return tmp;
        } else {
            return None;
        }
    }

    fn next_and(&mut self) -> Option<&'a Posting> {
        let mut focus = None; //Acts as temporary to be compared against
        let mut last_iter = self.operands.len() + 1; //The iterator that last set 'focus'
        'possible_documents: loop {
            // For every term
            for (i, input) in self.operands.iter_mut().enumerate() {
                'term_documents: loop {
                    // If the focus was set by the current iterator, we have a match
                    // We cycled through all the iterators once
                    if last_iter == i {
                        break 'term_documents;
                    }
                    // Get the next value for the current iterator
                    let v = unwrap_or_return_none!(input.next());
                    if focus.is_none() {
                        focus = Some(v);
                        last_iter = i;
                        break 'term_documents;
                    } else if v.0 < focus.unwrap().0 {
                        // If the value is smaller, get its next value
                        continue 'term_documents;
                    } else if v.0 == focus.unwrap().0 {
                        // If the value is equal, we are content. Proceed with the next iterator
                        break 'term_documents;
                    } else {
                        // If it is larger, we are now looking at a different focus.
                        // Reset focus and last_iter. Then start from the beginning
                        focus = Some(v);
                        last_iter = i;
                        continue 'possible_documents;
                    }
                }
            }
            return focus;
        }
    }

    fn estimate_length(&self) -> usize {
        match self.bool_operator {
            Some(BooleanOperator::And) => {
                return self.operands[0].estimate_length();
            }
            Some(BooleanOperator::Or) => {
                return self.operands[self.operands.len() - 1].estimate_length();
            }
            None => {
                match self.pos_operator {
                    Some(PositionalOperator::InOrder) => {
                        return self.operands[0].estimate_length();
                    }
                    None => {
                        unreachable!();
                    }
                }
            }
        }
    }
}


pub fn positional_intersect(lhs: &[usize],
                            rhs: &[usize],
                            bounds: (i64, i64))
                            -> Vec<(usize, usize)> {

    // To understand this algorithm imagine a table.
    // The columns are headed with the values from the rhs slice
    // The rows start with the values from the lhs slice
    // The value in each cell is its row value minus its column value
    // Example:

    // |   | 0 |  4 |  5 |  7 |
    // | 1 | 1 | -3 | -4 | -6 |
    // | 3 | 3 | -1 | -2 | -4 |
    // | 4 | 4 |  0 | -1 | -3 |
    // | 8 | 8 |  4 |  3 |  1 |

    // As both rhs and lhs are sorted we can assume two things:
    // 1. if we "go down" the result of the substraction is going to grow
    // 2. if we "go right" the result of the substraction is going to shrink

    // This algorithm walks through this table. If a difference is to great it will "go right"
    // Otherwise it will go down.
    // If a difference is inside the bounds it will check
    // to the left and to the right for adjacent matches

    let mut result = Vec::new();

    let mut lhs_ptr = 0;
    let mut rhs_ptr = 0;

    while lhs_ptr < lhs.len() && rhs_ptr < rhs.len() {
        let (lval, rval) = (lhs[lhs_ptr] as i64, rhs[rhs_ptr] as i64);
        let diff = lval - rval;
        if diff >= bounds.0 && diff <= bounds.1 {
            result.push((lhs[lhs_ptr], rhs[rhs_ptr]));

            // check "downwards"
            let mut d = lhs_ptr + 1;
            while d < lhs.len() && lhs[d] as i64 - rval <= bounds.1 {
                result.push((lhs[d], rhs[rhs_ptr]));
                d += 1;
            }

            // check "right"
            let mut r = rhs_ptr + 1;
            while r < rhs.len() && lval - rhs[r] as i64 >= bounds.0 {
                result.push((lhs[lhs_ptr], rhs[r]));
                r += 1;
            }

            rhs_ptr += 1;
            lhs_ptr += 1;
            continue;
        }
        if diff >= bounds.1 {
            rhs_ptr += 1;
        }
        if diff <= bounds.0 {
            lhs_ptr += 1;
        }
    }
    result
}



// --- Tests

#[cfg(test)]
mod tests {
    use super::*;
    use index::Index;

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
                                         BooleanQuery::Atom(QueryAtom::new(0, 0))],
                                   &None);
        assert!(qri.peek() == qri.peek());

    }

    #[test]
    fn empty_query() {
        let index = prepare_index();
        assert!(index.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 15))).document_ids == vec![]);

    }

    #[test]
    fn estimate_length() {
        let index = prepare_index();
        assert!(index.run_atom(0, &0).estimate_length() == 3);
        assert!(index.run_atom(0, &3).estimate_length() == 2);
        assert!(index.run_atom(0, &16).estimate_length() == 1);
        assert!(index.run_nary_query(&BooleanOperator::And,
                            &vec![BooleanQuery::Atom(QueryAtom::new(0, 3)),
                                  BooleanQuery::Atom(QueryAtom::new(0, 16))],
                            &None)
            .estimate_length() == 1);
        assert!(index.run_nary_query(&BooleanOperator::Or,
                            &vec![BooleanQuery::Atom(QueryAtom::new(0, 3)),
                                  BooleanQuery::Atom(QueryAtom::new(0, 16))],
                            &None)
            .estimate_length() == 2);
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

        assert!(index.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 7))).document_ids ==
                vec![0]);
        assert!(index.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 5))).document_ids ==
                vec![0, 2]);
        assert!(index.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 0))).document_ids ==
                vec![0, 1, 2]);
        assert!(index.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 16))).document_ids ==
                vec![1]);
    }

    #[test]
    fn nary_query() {
        let index = prepare_index();

        assert!(index.execute_query(&BooleanQuery::NAryQuery(BooleanOperator::And,
                                                    vec![BooleanQuery::Atom(QueryAtom::new(0,
                                                                                           5)),
                                                         BooleanQuery::Atom(QueryAtom::new(0,
                                                                                           0))],
                                                    None))
            .document_ids ==
                vec![0, 2]);
        assert!(index.execute_query(&BooleanQuery::NAryQuery(BooleanOperator::And,
                                                    vec![BooleanQuery::Atom(QueryAtom::new(0,
                                                                                           0)),
                                                         BooleanQuery::Atom(QueryAtom::new(0,
                                                                                           5))],
                                                    None))
            .document_ids ==
                vec![0, 2]);
    }

    #[test]
    fn and_query() {
        let index = prepare_index();
        assert!(index.execute_query(&BooleanQuery::NAryQuery(BooleanOperator::And,
                                                    vec![BooleanQuery::Atom(QueryAtom::new(0, 3)),
                                                         BooleanQuery::Atom(QueryAtom::new(0, 12))],
                                                    None))
            .document_ids == vec![]);
        assert!(index.execute_query(&BooleanQuery::NAryQuery(BooleanOperator::And,
                                                    vec![BooleanQuery::Atom(QueryAtom::new(0, 14)),
                                                         BooleanQuery::Atom(QueryAtom::new(0, 12))],
                                                    None))
            .document_ids == vec![1]);
        assert!(index.execute_query(
            &BooleanQuery::NAryQuery(
                BooleanOperator::And,
                vec![BooleanQuery::NAryQuery(BooleanOperator::And,
                    vec![BooleanQuery::Atom(QueryAtom::new(0, 3)),
                        BooleanQuery::Atom(QueryAtom::new(0, 9))],
                    None),
                    BooleanQuery::Atom(QueryAtom::new(0, 12))],
                None))
                .document_ids == vec![]);
        assert!(index.execute_query(
            &BooleanQuery::NAryQuery(
                BooleanOperator::And,
                vec![BooleanQuery::NAryQuery(BooleanOperator::And,
                    vec![BooleanQuery::Atom(QueryAtom::new(0, 2)),
                        BooleanQuery::Atom(QueryAtom::new(0, 4))],
                    None),
                    BooleanQuery::Atom(QueryAtom::new(0, 16))],
                None))
                .document_ids == vec![1]);
    }

    #[test]
    fn or_query() {
        let index = prepare_index();
        assert!(index.execute_query(&BooleanQuery::NAryQuery(BooleanOperator::Or,
                                                    vec![BooleanQuery::Atom(QueryAtom::new(0, 3)),
                                                         BooleanQuery::Atom(QueryAtom::new(0, 12))],
                                                    None))
            .document_ids == vec![0, 1, 2]);
        assert!(index.execute_query(&BooleanQuery::NAryQuery(BooleanOperator::Or,
                                                    vec![BooleanQuery::Atom(QueryAtom::new(0, 14)),
                                                         BooleanQuery::Atom(QueryAtom::new(0, 12))],
                                                    None))
            .document_ids == vec![1]);
        assert!(index.execute_query(
            &BooleanQuery::NAryQuery(
                BooleanOperator::Or,
                vec![BooleanQuery::NAryQuery(BooleanOperator::Or,
                    vec![BooleanQuery::Atom(QueryAtom::new(0, 3)),
                        BooleanQuery::Atom(QueryAtom::new(0, 9))],
                    None),
                    BooleanQuery::Atom(QueryAtom::new(0, 16))],
                None))
                .document_ids == vec![0,1,2]);
    }

    #[test]
    fn inorder_query() {
        let index = prepare_index();
        assert!(index.execute_query(&BooleanQuery::PositionalQuery(PositionalOperator::InOrder,
                                                          vec![QueryAtom::new(0, 0),
                                                               QueryAtom::new(1, 1)]))
            .document_ids == vec![0]);
        assert!(index.execute_query(&BooleanQuery::PositionalQuery(PositionalOperator::InOrder,
                                                          vec![QueryAtom::new(1, 0),
                                                               QueryAtom::new(0, 1)]))
            .document_ids == vec![2]);
        assert!(index.execute_query(&BooleanQuery::PositionalQuery(PositionalOperator::InOrder,
                                                          vec![QueryAtom::new(0, 0),
                                                               QueryAtom::new(1, 2)]))
            .document_ids == vec![1]);

        assert!(index.execute_query(&BooleanQuery::PositionalQuery(PositionalOperator::InOrder,
                                                          vec![QueryAtom::new(2, 2),
                                                               QueryAtom::new(1, 1),
                                                               QueryAtom::new(0, 0)]))
            .document_ids == vec![0]);
        assert!(index.execute_query(&BooleanQuery::PositionalQuery(PositionalOperator::InOrder,
                                                          vec![QueryAtom::new(0, 2),
                                                               QueryAtom::new(1, 1),
                                                               QueryAtom::new(2, 0)]))
            .document_ids == vec![2]);
        assert!(index.execute_query(&BooleanQuery::PositionalQuery(PositionalOperator::InOrder,
                                                          vec![QueryAtom::new(0, 2),
                                                               QueryAtom::new(1, 1),
                                                               QueryAtom::new(3, 0)]))
            .document_ids == vec![]);
    }

    #[test]
    fn query_filter() {
        let index = prepare_index();
        println!("{:?}", index.execute_query(
            &BooleanQuery::NAryQuery(BooleanOperator::And,
                                     vec![BooleanQuery::Atom(QueryAtom::new(0, 2)),
                                          BooleanQuery::Atom(QueryAtom::new(0, 0))],
                                     Some(
                                         (FilterOperator::Not,
                                          Box::new(
                                              BooleanQuery::Atom(
                                                  QueryAtom::new(0, 16))))))).document_ids);
        assert!(index.execute_query(
            &BooleanQuery::NAryQuery(
                BooleanOperator::And,
                vec![BooleanQuery::Atom(QueryAtom::new(0, 2)),
                     BooleanQuery::Atom(QueryAtom::new(0, 0))],
                Some((FilterOperator::Not,
                      Box::new(BooleanQuery::Atom(
                          QueryAtom::new(0, 16))))))).document_ids == vec![0,2]);



    }

}
