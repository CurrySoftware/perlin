use std::hash::Hash;

use perlin_core::index::posting::{Posting, PostingIterator, PostingDecoder};
use perlin_core::utils::seeking_iterator::{PeekableSeekable, SeekingIterator};
use perlin_core::utils::progress::Progress;

use field::Field;
pub use query::operators::{Or, And, SplitFunnel, Funnel, Combinator};

#[macro_use]
pub mod query_pipeline;
mod operators;

#[derive(Debug, Copy, Clone)]
pub enum ChainingOperator {
    Must,
    May,
    MustNot,
}

#[derive(Clone)]
pub enum Operand<'a> {
    Empty,
    Term(PostingDecoder<'a>),
    Operated(Box<Operator>, Vec<PeekableSeekable<Operand<'a>>>),
}

//Stolen from SO: https://stackoverflow.com/questions/30353462/how-to-clone-a-struct-storing-a-trait-object
pub trait OperatorClone {
    fn clone_box(&self) -> Box<Operator>;
}

impl<T> OperatorClone for T where T: 'static + Operator + Clone {
    fn clone_box(&self) -> Box<Operator> {
        Box::new(self.clone())
    }
}

impl Clone for Box<Operator> {
    fn clone(&self) -> Box<Operator> {
        self.clone_box()
    }
}


impl<'a> Iterator for Operand<'a> {
    type Item = Posting;

    fn next(&mut self) -> Option<Posting> {
        match *self {
            Operand::Empty => None,
            Operand::Term(ref mut decoder) => decoder.next(),
            Operand::Operated(ref mut operator, ref mut operands) => operator.next(operands),
        }
    }
}

impl<'a> SeekingIterator for Operand<'a> {
    type Item = Posting;

    fn next_seek(&mut self, other: &Posting) -> Option<Posting> {
        match *self {
            Operand::Empty => None,
            Operand::Term(ref mut decoder) => decoder.next_seek(other),
            Operand::Operated(ref mut operator, ref mut operands) => {
                operator.next_seek(operands, other)
            }
        }
    }
}

impl<'a> Operand<'a> {
    pub fn progress(&self) -> Progress {
        match *self {
            Operand::Empty => Progress::done(),
            Operand::Term(ref decoder) => decoder.progress(),
            Operand::Operated(ref operator, ref operands) => {
                operator.progress(operands)
            }

        }
    }
}

pub trait Operator: OperatorClone {
    fn next(&mut self, operands: &mut [PeekableSeekable<Operand>]) -> Option<Posting>;
    fn next_seek(&mut self,
                 operands: &mut [PeekableSeekable<Operand>],
                 other: &Posting)
                 -> Option<Posting>;

    fn progress(&self,
                    operands: &[PeekableSeekable<Operand>]) -> Progress;
}

pub trait ToOperands<'a> {
    fn to_operands(self) -> Vec<(ChainingOperator, PeekableSeekable<Operand<'a>>)>;
}

pub struct QueryTerm<'a, T: 'a + Hash + Eq> {
    field: &'a Field<T>,
    value: T,
}

impl<'a, T: 'a + Hash + Eq + Ord> QueryTerm<'a, T> {
    pub fn create(field: &'a Field<T>, value: T) -> Self {
        QueryTerm {
            field: field,
            value: value,
        }
    }

    pub fn apply(&self) -> PostingIterator<'a> {
        self.field.query_atom(&self.value)
    }
}

#[derive(Clone)]
pub struct Query<'a> {
    pub query: String,
    pub filter: Vec<(ChainingOperator, PeekableSeekable<Operand<'a>>)>,
}

impl<'a> Query<'a> {
    pub fn new(query: String) -> Self {
        Query {
            query: query,
            filter: vec![],
        }
    }

    pub fn filter_by(mut self, operator: ChainingOperator, filter: PostingIterator<'a>) -> Self {
        match filter {
            PostingIterator::Empty => {
                self.filter.push((operator, PeekableSeekable::new(Operand::Empty)))
            }
            PostingIterator::Decoder(decoder) => {
                self.filter.push((operator, PeekableSeekable::new(Operand::Term(decoder))))
            }
        }
        self
    }
}
