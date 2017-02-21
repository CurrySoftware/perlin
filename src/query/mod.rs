#[macro_use]
pub mod query_pipeline;

mod operators;

use perlin_core::index::posting::{Posting};

use language::CanApply;

pub use query::operators::{Or, And, Funnel, Operator}; 

/// An Operand is just something emmiting postings!
pub type Operand<'a> = Box<Iterator<Item = Posting> + 'a>;

pub trait ToOperand<'a> {
    fn to_operand(self) -> Operand<'a>;
}

pub trait ToBinaryOperand<'a> {
    fn to_bin_operand(self, other: Operand<'a>) -> Operand<'a>;
}

pub struct AndConstructor<CB> {
    cb: CB
}

impl<CB> AndConstructor<CB> {
    pub fn create(cb: CB) -> Self {
        AndConstructor {
            cb: cb
        }
    }
}

impl<CB, T> CanApply<T> for AndConstructor<CB>
    where CB: CanApply<T>
{
    type Output = CB::Output;
    fn apply(&mut self, t: T) {
        self.cb.apply(t)
    }
}

impl<'a, CB> ToBinaryOperand<'a> for AndConstructor<CB>
    where CB: ToOperand<'a>
{
    fn to_bin_operand(self, op: Operand<'a>) -> Operand<'a> {
        Box::new(And::create(vec![self.cb.to_operand(), op]))
    }
}

pub struct OrConstructor<CB> {
    cb: CB
}

impl<CB> OrConstructor<CB> {
    pub fn create(cb: CB) -> Self {
        OrConstructor {
            cb: cb
        }
    }
}

impl<CB, T> CanApply<T> for OrConstructor<CB>
    where CB: CanApply<T>
{
    type Output = CB::Output;
    fn apply(&mut self, t: T) {
        self.cb.apply(t)
    }
}

impl<'a, CB> ToBinaryOperand<'a> for OrConstructor<CB>
    where CB: ToOperand<'a>
{
    fn to_bin_operand(self, op: Operand<'a>) -> Operand<'a>{
        Box::new(Or::create(vec![self.cb.to_operand(), op]))
    }
}
