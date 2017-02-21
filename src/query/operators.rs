use std::hash::Hash;

use perlin_core::index::posting::{Posting, PostingIterator};
use perlin_core::index::Index;

use language::CanApply;
use query::{ToOperand, Operand};

/// Mimics the functionality of the `try!` macro for `Option`s.
/// Evaluates `Some(x)` to x. Else it returns `None`.
macro_rules! try_option{
    ($operand:expr) => {
        if let Some(x) = $operand {
            x
        } else {
            return None;
        }
    }
}

pub enum Operator {
    All,
    Any,
}

/// This funnel is used at an end of a query pipeline
/// It calls `index.query_atom` and stores the result, which is lazy
/// When `to_operand` is then called, it packs everything into an operator!
pub struct Funnel<'a, T: 'a + Hash + Eq> {
    index: &'a Index<T>,
    operator: Operator,
    result: Vec<PostingIterator<'a>>,
}

impl<'a, T: 'a + Hash + Eq> Funnel<'a, T> {
    pub fn create(operator: Operator, index: &'a Index<T>) -> Self {
        Funnel {
            index: index,
            operator: operator,
            result: Vec::new(),
        }
    }
}

impl<'a: 'b, 'b, T: 'a + Hash + Eq + Ord> CanApply<&'b T> for Funnel<'a, T> {
    type Output = T;

    fn apply(&mut self, term: &T) {
        if let Some(posting_iter) = self.index.query_atom(term) {
            self.result.push(posting_iter);
        }
    }
}

impl<'a, T: 'a + Hash + Eq + Ord> CanApply<T> for Funnel<'a, T> {
    type Output = T;

    fn apply(&mut self, term: T) {
        if let Some(posting_iter) = self.index.query_atom(&term) {
            self.result.push(posting_iter);
        }
    }
}

impl<'a, T: 'a + Hash + Eq> ToOperand<'a> for Funnel<'a, T> {
    fn to_operand(self) -> Operand<'a> {
        match self.operator {
            Operator::All => {
                Box::new(And {
                    operands: self.result
                        .into_iter()
                        .map(|piter| Box::new(piter) as Box<Iterator<Item = Posting>>)
                        .collect::<Vec<_>>(),
                })
            }
            Operator::Any => {
                Box::new(Or::create(self.result
                    .into_iter()
                    .map(|piter| Box::new(piter) as Box<Iterator<Item = Posting>>)
                    .collect::<Vec<_>>()))
            }
        }

    }
}
/// END FUNNEL


pub struct And<'a> {
    operands: Vec<Operand<'a>>,
}

impl<'a> And<'a>{
    pub fn create(operands: Vec<Operand<'a>>) -> And {
        And {
            operands: operands
        }
    }
}

impl<'a> Iterator for And<'a> {
    type Item = Posting;

    fn next(&mut self) -> Option<Self::Item> {
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
                    let v = try_option!(input.next());
                    if focus.is_none() {
                        focus = Some(v);
                        last_iter = i;
                        break 'term_documents;
                    } else if v.0 < focus.unwrap().0 {
                        // If the value is smaller, get its next value
                        continue 'term_documents;
                    } else if v.0 > focus.unwrap().0 {
                        // If it is larger, we are now looking at a different focus.
                        // Reset focus and last_iter. Then start from the beginning
                        focus = Some(v);
                        last_iter = i;
                        continue 'possible_documents;
                    } else {
                        // If the value is equal, we are content. Proceed with the next iterator
                        break 'term_documents;
                    }

                }
            }
            return focus;
        }
    }
}

pub struct Or<'a> {
    operands: Vec<Operand<'a>>,
    buf: Vec<Posting>,
}

impl<'a> Or<'a> {
    pub fn create(operands: Vec<Operand<'a>>) -> Or {
        Or {
            operands: operands,
            buf: Vec::new(),
        }
    }
}

impl<'a> Iterator for Or<'a> {
    type Item = Posting;
    fn next(&mut self) -> Option<Self::Item> {
        // Find the smallest current value of all operands
        self.buf.append(&mut self.operands
            .iter_mut()
            .map(|op| op.next())
            .filter(|val| val.is_some())
            .map(|val| val.unwrap())
            .collect());
        self.buf.sort();
        if self.buf.is_empty() {
            None
        } else {
            Some(self.buf.swap_remove(0))
        }
    }
}
