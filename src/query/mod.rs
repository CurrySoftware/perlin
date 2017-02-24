#[macro_use]
pub mod query_pipeline;

mod operators;

use perlin_core::index::posting::Posting;

pub use query::operators::{Or, And, SplitFunnel, Funnel, Operator};

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
