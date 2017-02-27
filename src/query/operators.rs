use std::hash::Hash;
use std::fmt::Debug;

use perlin_core::index::posting::{Posting, PostingIterator};
use perlin_core::index::Index;

use language::CanApply;
use query::{ToOperands, Operand, ChainedOperand, ChainingOperator};

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

#[derive(Debug)]
pub enum Operator {
    All,
    Any,
}


pub struct SplitFunnel<'a, T: 'a + Hash + Eq, CB> {
    index: &'a Index<T>,
    operator: Operator,
    chaining_operator: ChainingOperator,
    result: Vec<PostingIterator<'a>>,
    callback: CB,
}

impl<'a, T: 'a + Hash + Eq, CB> SplitFunnel<'a, T, CB> {
    pub fn create(chaining_operator: ChainingOperator,
                  operator: Operator,
                  index: &'a Index<T>,
                  cb: CB)
                  -> Self {
        SplitFunnel {
            index: index,
            operator: operator,
            chaining_operator: chaining_operator,
            callback: cb,
            result: Vec::new(),
        }
    }
}

impl<'a, T: 'a + Hash + Eq + Ord, CB> CanApply<T> for SplitFunnel<'a, T, CB>
    where CB: CanApply<T>
{
    type Output = CB::Output;

    fn apply(&mut self, term: T) {
        // Query index
        self.result.push(self.index.query_atom(&term));
        // And keep going
        self.callback.apply(term);
    }
}


impl<'a, T: 'a + Hash + Eq, CB> ToOperands<'a> for SplitFunnel<'a, T, CB>
    where CB: ToOperands<'a>
{
    fn to_operands(self) -> Vec<ChainedOperand<'a>>{
        let mut other = self.callback.to_operands();
        match self.operator {
            Operator::All => {
                other.push(
                    (self.chaining_operator,
                      Box::new(And {
                          operands: self.result
                              .into_iter()
                              .map(|piter| Box::new(piter) as Box<Iterator<Item = Posting>>)
                              .collect::<Vec<_>>(),
                      })))
            },
            Operator::Any => {
                other.push((self.chaining_operator,
                      Box::new(Or::create(self.result
                          .into_iter()
                          .map(|piter| Box::new(piter) as Box<Iterator<Item = Posting>>)
                          .collect::<Vec<_>>()))))
            }
        }
        other
    }
}

/// This funnel is used at an end of a query pipeline
/// It calls `index.query_atom` and stores the result, which is lazy
/// When `to_operand` is then called, it packs everything into an operator!
//TODO: In this struct lies an opportunity to optimize
//If the operator is AND and one term returns an empty posting iterator
//We could skip the rest
//If the operator is Or and one term returns an empty posting iterator we could discard it
pub struct Funnel<'a, T: 'a + Hash + Eq> {
    index: &'a Index<T>,
    operator: Operator,
    chaining_operator: ChainingOperator,
    result: Vec<PostingIterator<'a>>,
}

impl<'a, T: 'a + Hash + Eq> Funnel<'a, T> {
    pub fn create(chaining_operator: ChainingOperator,
                  operator: Operator,
                  index: &'a Index<T>)
                  -> Self {
        Funnel {
            index: index,
            operator: operator,
            chaining_operator: chaining_operator,
            result: Vec::new(),
        }
    }
}

impl<'a: 'b, 'b, T: 'a + Hash + Eq + Ord> CanApply<&'b T> for Funnel<'a, T> {
    type Output = T;

    fn apply(&mut self, term: &T) {
        self.result.push(self.index.query_atom(term));
    }
}

impl<'a, T: 'a + Hash + Eq + Ord + Debug> CanApply<T> for Funnel<'a, T> {
    type Output = T;

    fn apply(&mut self, term: T) {
        self.result.push(self.index.query_atom(&term));
    }
}

impl<'a, T: 'a + Hash + Eq> ToOperands<'a> for Funnel<'a, T> {
    fn to_operands(self) -> Vec<ChainedOperand<'a>> {
        if self.result.is_empty() {
            return vec![];
        }
        match self.operator {
            Operator::All => {
                vec![(self.chaining_operator,
                      Box::new(And {
                          operands: self.result
                              .into_iter()
                              .map(|piter| Box::new(piter) as Box<Iterator<Item = Posting>>)
                              .collect::<Vec<_>>(),
                      }))]
            }
            Operator::Any => {
                vec![(self.chaining_operator,
                      Box::new(Or::create(self.result
                          .into_iter()
                          .map(|piter| Box::new(piter) as Box<Iterator<Item = Posting>>)
                          .collect::<Vec<_>>())))]
            }
        }

    }
}
/// END FUNNEL


pub struct And<'a> {
    operands: Vec<Operand<'a>>,
}

impl<'a> And<'a> {
    pub fn create(operands: Vec<Operand<'a>>) -> And {
        And { operands: operands }
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
