use std::hash::Hash;

use perlin_core::index::Index;
use perlin_core::index::posting::{Posting, PostingIterator};

use language::CanApply;

pub type Operand<'a> = Box<Iterator<Item = Posting> + 'a>;

pub trait ToOperand<'a> {
    fn to_operand(self) -> Operand<'a>;
}

pub trait ToBinaryOperand<'a> {
    fn to_bin_operand(self, other: Operand<'a>) -> Operand<'a>;
}

pub enum Operator {
    And,
    Or,
}

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
        Box::new(FunneledAnd {
            operands: self.result
                .into_iter()
                .map(|piter| Box::new(piter) as Box<Iterator<Item = Posting>>)
                .collect::<Vec<_>>(),
        })
    }
}


pub struct FunneledAnd<'a> {
    operands: Vec<Operand<'a>>,
}

impl<'a> Iterator for FunneledAnd<'a> {
    type Item = Posting;

    fn next(&mut self) -> Option<Self::Item> {
        self.operands[0].next()
    }
}

pub struct Or<'a> {
    operands: Vec<Operand<'a>>
}

impl<'a> Iterator for Or<'a> {
    type Item = Posting;
    fn next(&mut self) -> Option<Self::Item> {
        self.operands[0].next()
    }
}

pub struct BinaryOr<CB> {
    cb: CB
}

impl<CB> BinaryOr<CB> {
    pub fn create(cb: CB) -> Self {
        BinaryOr {
            cb: cb
        }
    }
}

impl<CB, T> CanApply<T> for BinaryOr<CB>
    where CB: CanApply<T>
{
    type Output = CB::Output;
    fn apply(&mut self, t: T) {
        self.cb.apply(t)
    }
}

impl<'a, CB> ToBinaryOperand<'a> for BinaryOr<CB>
    where CB: ToOperand<'a>
{
    fn to_bin_operand(self, op: Operand<'a>) -> Operand<'a>{
        Box::new(Or {
            operands: vec![self.cb.to_operand(), op]                
        })
    }
}
