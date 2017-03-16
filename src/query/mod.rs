use std::hash::Hash;

use perlin_core::index::posting::{Posting, PostingIterator};

use field::Field;
pub use query::operators::{Or, And, SplitFunnel, Funnel, Operator};

#[macro_use]
pub mod query_pipeline;
mod operators;

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

pub struct QueryTerm<'a, T: 'a + Hash + Eq> {
    field: &'a Field<T>,
    value: T
}

impl<'a, T: 'a + Hash + Eq + Ord> QueryTerm<'a, T> {
    pub fn create(field: &'a Field<T>, value: T) -> Self {
        QueryTerm {
            field: field,
            value: value
        }
    }

    pub fn apply(&self) -> PostingIterator<'a> {
        self.field.query_atom(&self.value)
    }
}

pub struct Query<'a> {
    pub query: String,
    pub filter: Vec<ChainedOperand<'a>>
}

impl<'a> Query<'a> {
    pub fn new(query: String) -> Self {
        Query{
            query: query,
            filter: vec![]
        }
    }

    pub fn filter(mut self, filter: PostingIterator<'a>) -> Self {
        self.filter.push((ChainingOperator::Must, Box::new(filter) as Box<Iterator<Item = Posting>>));
        self
    }
}
