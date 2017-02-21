#[macro_use]
pub mod query_pipeline;

mod operators;

use perlin_core::index::posting::Posting;

use language::CanApply;

pub use query::operators::{Or, And, Funnel, Operator};

pub enum ChainingOperator {
    Must,
    May,
    MustNot
}

/// An Operand is just something emmiting postings!
pub type Operand<'a> = Box<Iterator<Item = Posting> + 'a>;
pub type ChainedOperand<'a> = (ChainingOperator, Box<Iterator<Item = Posting> + 'a>);

pub trait ToOperands<'a> {
    fn to_operands(self) -> Vec<ChainedOperand<'a>>;
}

pub struct Chain<CB1, CB2> {
    cb1: CB1,
    cb2: CB2,
}

impl<CB1, CB2> Chain<CB1, CB2> {
    pub fn create(cb1: CB1, cb2: CB2) -> Self {
        Chain {
            cb1: cb1,
            cb2: cb2,
        }
    }
}

impl<CB1, CB2, T> CanApply<T> for Chain<CB1, CB2>
    where CB1: for<'r> CanApply<&'r T>,
          CB2: for<'r> CanApply<&'r T>
{
    type Output = T;

    fn apply(&mut self, input: T) {
        self.cb1.apply(&input);
        self.cb2.apply(&input);
    }
}

impl<'a, CB1, CB2> ToOperands<'a> for Chain<CB1, CB2>
    where CB1: ToOperands<'a>,
          CB2: ToOperands<'a>
{
    fn to_operands(self) -> Vec<ChainedOperand<'a>> {
        let mut result = self.cb1.to_operands();
        result.append(&mut self.cb2.to_operands());
        result
    }
}
