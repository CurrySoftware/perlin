use perlin_core::index::posting::{Posting, PostingIterator, PostingDecoder};
use perlin_core::utils::seeking_iterator::{PeekableSeekable, SeekingIterator};
use perlin_core::utils::progress::Progress;

pub use query::operators::{And, Funnel, Combinator};

#[macro_use]
pub mod query_pipeline;
mod operators;

#[derive(Debug, Copy, Clone)]
pub enum ChainingOperator {
    Must,
    May,
    MustNot,
}

#[derive(Debug, Copy, Clone)]
pub struct Weight(pub f32);

#[derive(Clone)]
pub enum Operand<'a> {
    Empty,
    Term(Weight, PostingDecoder<'a>),
    Operated(Weight, Vec<PeekableSeekable<Operand<'a>>>),
}

impl<'a> Iterator for Operand<'a> {
    type Item = Posting;

    fn next(&mut self) -> Option<Posting> {
        match *self {
            Operand::Empty => None,
            Operand::Term(_, ref mut decoder) => decoder.next(),
            Operand::Operated(_, ref mut operands) => And::next(operands),
        }
    }
}

impl<'a> SeekingIterator for Operand<'a> {
    type Item = Posting;

    fn next_seek(&mut self, other: &Posting) -> Option<Posting> {
        match *self {
            Operand::Empty => None,
            Operand::Term(_, ref mut decoder) => decoder.next_seek(other),
            Operand::Operated(_, ref mut operands) => {
                And::next_seek(operands, other)
            }
        }
    }
}

impl<'a> Operand<'a> {
    pub fn progress(&self) -> Progress {
        match *self {
            Operand::Empty => Progress::done(),
            Operand::Term(_, ref decoder) => decoder.progress(),
            Operand::Operated(_, ref operands) => {
                And::progress(operands)
            }

        }
    }
}

pub trait ToOperands<'a> {
    fn to_operands(self) -> Vec<(ChainingOperator, PeekableSeekable<Operand<'a>>)>;
}

#[derive(Clone)]
pub struct Query<'a> {
    pub query: &'a str,
    pub filter: Vec<(ChainingOperator, PeekableSeekable<Operand<'a>>)>,
}

impl<'a> Query<'a> {
    pub fn new(query: &'a str) -> Self {
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
                self.filter.push((operator, PeekableSeekable::new(Operand::Term(Weight(1.0), decoder))))
            }
        }
        self
    }
}
