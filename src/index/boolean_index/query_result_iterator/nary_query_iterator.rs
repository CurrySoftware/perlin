use utils::seeking_iterator::{PeekableSeekable, SeekingIterator};

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

    //TODO: This has to be completely rewritten
    fn next_inorder(&mut self) -> Option<Posting> {
        None
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
