use utils::owning_iterator::{PeekableSeekable, SeekingIterator};

use index::boolean_index::boolean_query::*;
use index::boolean_index::posting::Posting;
use index::boolean_index::query_result_iterator::*;

pub struct NAryQueryIterator<'a> {
    pos_operator: Option<PositionalOperator>,
    bool_operator: Option<BooleanOperator>,
    operands: Vec<PeekableSeekable<QueryResultIterator<'a>>>,
}


impl<'a> Iterator for NAryQueryIterator<'a> {
    type Item = Posting;


    fn next(&mut self) -> Option<Self::Item> {
        match self.bool_operator {
            Some(BooleanOperator::And) => self.next_and(),
            Some(BooleanOperator::Or) => self.next_or(),
            None => {
                match self.pos_operator {
                    Some(PositionalOperator::InOrder) => self.next_inorder(),
                    None => {
                        unreachable!(false);
                    }
                }
            }
        }
    }
}

impl<'a> SeekingIterator for NAryQueryIterator<'a> {
    type Item = Posting;

    fn next_seek(&mut self, target: &Self::Item) -> Option<Self::Item> {
        //Advance operands to `target`
        for op in &mut self.operands {
            op.peek_seek(target);
        }
        self.next()
    }
}

impl<'a> NAryQueryIterator<'a> {
    pub fn new_positional(operator: PositionalOperator, operands: Vec<PeekableSeekable<QueryResultIterator>>) -> NAryQueryIterator {
        let mut result = NAryQueryIterator {
            pos_operator: Some(operator),
            bool_operator: None,
            operands: operands       
        };
        result.operands.sort_by_key(|op| op.inner().estimate_length());
        result
    }


    pub fn new(operator: BooleanOperator, operands: Vec<PeekableSeekable<QueryResultIterator>>) -> NAryQueryIterator {
        let mut result = NAryQueryIterator {
            pos_operator: None,
            bool_operator: Some(operator),
            operands: operands
        };
        result.operands.sort_by_key(|op| op.inner().estimate_length());
        result
    }

    pub fn estimate_length(&self) -> usize {
        match self.bool_operator {
            Some(BooleanOperator::And) => self.operands[0].inner().estimate_length(),
            Some(BooleanOperator::Or) => self.operands[self.operands.len() - 1].inner().estimate_length(),
            None => {
                match self.pos_operator {
                    Some(PositionalOperator::InOrder) => self.operands[0].inner().estimate_length(),
                    None => {
                        unreachable!();
                    }
                }
            }
        }
    }

    fn next_inorder(&mut self) -> Option<Posting> {
        let mut focus = try_option!(self.operands[0].next()); //Acts as temporary to be compared against
        let mut focus_positions = focus.positions().clone();
        // The iterator index that last set 'focus'
        let mut last_doc_iter = 0;
        // The relative position of the term in last_doc_iter
        let mut last_positions_iter = self.operands[0].inner().relative_position();
        'possible_documents: loop {
            // For every term
            for (i, input) in self.operands.iter_mut().enumerate() {
                // If the focus was set by the current iterator, we have a match
                if last_doc_iter == i {
                    continue;
                }
                // Get the next doc_id >= focus for the current iterator
                let mut v = try_option!(input.next_seek(&focus));
                if v.0 == focus.0 {
                    // If the doc_id is equal, check positions
                    let position_offset = last_positions_iter as i64 - input.inner().relative_position() as i64;
                    focus_positions = positional_intersect(&focus_positions,
                                                           v.positions(),
                                                           (position_offset, position_offset))
                        .iter()
                        .map(|pos| pos.1)
                        .collect::<Vec<_>>();
                    last_positions_iter = input.inner().relative_position();
                    if focus_positions.is_empty() {
                        // No suitable positions found. Next document
                        v = try_option!(input.next());
                    } else {
                        continue;
                    }
                }
                // If it is larger or no positions matched, we are now looking at a different focus.
                // Reset focus and last_iter. Then start from the beginning
                focus = v;
                focus_positions = focus.positions().clone();
                last_doc_iter = i;
                last_positions_iter = input.inner().relative_position();
                continue 'possible_documents;
            }
            return Some(focus);
        }
    }

    fn next_or(&mut self) -> Option<Posting> {
        // TODO: Probably improveable
        // Find the smallest current value of all operands
        let min_doc_id = self.operands
            .iter_mut()
            .map(|op| op.peek())
            .filter(|val| val.is_some())
            .map(|val| *val.unwrap().doc_id())
            .min();

        // Walk over all operands. Advance those who emit the min value
        // Kick out thos who emit None
        if min_doc_id.is_some() {
            let mut i = 0;
            let mut tmp = None;
            while i < self.operands.len() {
                let v = self.operands[i].peek().map(|p| *p.doc_id());
                if v.is_none() {
                    self.operands.swap_remove(i);
                    continue;
                } else if v == min_doc_id {
                    tmp = self.operands[i].next();
                }
                i += 1;
            };
            tmp
        } else {
            None
        }
    }

    fn next_and(&mut self) -> Option<Posting> {
        let mut focus = try_option!(self.operands[0].next()); //Acts as temporary to be compared against
        let mut last_iter = 0; //The iterator that last set 'focus'
        'possible_documents: loop {
            // For every term
            for (i, input) in self.operands.iter_mut().enumerate() {
                // If the focus was set by the current iterator, we have a match
                // We cycled through all the iterators once
                if last_iter == i {
                    continue;
                }
                
                let v = try_option!(input.next_seek(&focus));
                if v.0 > focus.0 {
                    // If it is larger, we are now looking at a different focus.
                    // Reset focus and last_iter. Then start from the beginning
                    focus = v;
                    last_iter = i;
                    continue 'possible_documents;
                }
            }
            return Some(focus);
        }
    }
}


pub fn positional_intersect(lhs: &[u32], rhs: &[u32], bounds: (i64, i64)) -> Vec<(u32, u32)> {

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

    // This algorithm walks through this table. If a difference is to great it will
    // "go right"
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
