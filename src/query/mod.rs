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

pub struct Chain<CB1, CB2> {
    cb1: CB1,
    cb2: CB2
}

impl<CB1, CB2> Chain<CB1, CB2>  {
    pub fn create(cb1: CB1, cb2: CB2) -> Self {
        Chain {
            cb1: cb1,
            cb2: cb2
        }
    }
}

impl<CB1, CB2, T: Copy> CanApply<T> for Chain<CB1, CB2>
    where CB1: CanApply<T>,
          CB2: CanApply<T> {
    type Output = CB1::Output;

    fn apply(&mut self, input: T) {
        self.cb1.apply(input);
        self.cb2.apply(input);
    }
}

impl<'a, CB1, CB2> ToOperand<'a> for  Chain<CB1, CB2> 
    where CB1: ToBinaryOperand<'a>,
          CB2: ToOperand<'a>
    {
    fn to_operand(self) -> Operand<'a> {
        self.cb1.to_bin_operand(self.cb2.to_operand())
    }
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
