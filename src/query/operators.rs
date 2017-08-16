use std::hash::Hash;
use std::fmt::Debug;
use std::marker::PhantomData;

use perlin_core::index::posting::{Posting, PostingIterator};
use perlin_core::utils::seeking_iterator::{PeekableSeekable, SeekingIterator};
use perlin_core::utils::progress::Progress;

use language::CanApply;
use query::{Weight, ToOperands, Operand, ChainingOperator};
use field::{Field, Fields};

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
pub enum Combinator {
    All,
    Any,
}

/// This funnel is used at an end of a query pipeline
/// It calls `index.query_atom` and stores the result, which is lazy
/// When `to_operand` is then called, it packs everything into an operator!
// TODO: In this struct lies an opportunity to optimize
// If the operator is AND and one term returns an empty posting iterator
// We could skip the rest
// If the operator is Or and one term returns an empty posting iterator we
// could discard it
pub struct Funnel<'a, T: 'a, TIndex: 'a> {
    index: &'a TIndex,
    chaining_operator: ChainingOperator,
    result: Vec<PeekableSeekable<Operand<'a>>>,
    _term: PhantomData<T>,
}

impl<'a, T: 'a, TIndex: 'a> Funnel<'a, T, TIndex> {
    pub fn create(chaining_operator: ChainingOperator, index: &'a TIndex) -> Self {
        Funnel {
            index,
            chaining_operator,
            result: Vec::new(),
            _term: PhantomData,
        }
    }
}

impl<'a: 'b, 'b, T: 'a + Hash + Eq + Ord + Debug> CanApply<&'b T> for Funnel<'a, T, Fields<T>> {
    type Output = T;

    fn apply(&mut self, term: &'b T) {
        for (key, index) in self.index.fields.iter() {
            let w = index.term_doc_ratio;
            match index.query_atom(&term) {
                (_, PostingIterator::Empty) => {
                    self.result.push(PeekableSeekable::new(Operand::Empty))
                }
                (idf, PostingIterator::Decoder(decoder)) => {
                    println!("Term {:?} in field {:?} queried with a weight of: {:?}.",
                             &term,
                             key,
                             Weight(idf.0 * w));
                    self.result.push(PeekableSeekable::new(Operand::Term(Weight(idf.0 * w),
                                                                         decoder)))
                }
            }
        }
    }
}


impl<'a, T: 'a + Hash + Eq + Ord + Debug> CanApply<T> for Funnel<'a, T, Fields<T>> {
    type Output = T;

    fn apply(&mut self, term: T) {
        for (key, index) in self.index.fields.iter() {
            let w = index.term_doc_ratio;;
            match index.query_atom(&term) {
                (_, PostingIterator::Empty) => {
                    self.result.push(PeekableSeekable::new(Operand::Empty))
                }
                (idf, PostingIterator::Decoder(decoder)) => {
                    println!("Term {:?} in field {:?} queried with a weight of: {:?}.",
                             &term,
                             key,
                             Weight(idf.0 * w));
                    self.result.push(PeekableSeekable::new(Operand::Term(Weight(idf.0 * w),
                                                                         decoder)))
                }
            }
        }
    }
}


impl<'a: 'b, 'b, T: 'a + Hash + Eq + Ord + Debug> CanApply<&'b T> for Funnel<'a, T, Field<T>>
{
    type Output = T;

    fn apply(&mut self, term: &'b T) {
        let w = self.index.term_doc_ratio;
        match self.index.query_atom(&term) {
            (_, PostingIterator::Empty) => self.result.push(PeekableSeekable::new(Operand::Empty)),
            (idf, PostingIterator::Decoder(decoder)) => {
                println!("Term {:?} queried with a weight of: {:?}.",
                         &term,
                         Weight(idf.0 * w));
                self.result.push(PeekableSeekable::new(Operand::Term(Weight(idf.0 * w), decoder)))
            }
        }
    }
}


impl<'a, T: 'a + Hash + Eq + Ord + Debug> CanApply<T> for Funnel<'a, T, Field<T>>
{
    type Output = T;

    fn apply(&mut self, term: T) {
        let w = self.index.term_doc_ratio;
        match self.index.query_atom(&term) {
            (_, PostingIterator::Empty) => self.result.push(PeekableSeekable::new(Operand::Empty)),
            (idf, PostingIterator::Decoder(decoder)) => {
                println!("Term {:?} queried with a weight of: {:?}.",
                         &term,
                         Weight(idf.0 * w));
                self.result.push(PeekableSeekable::new(Operand::Term(Weight(idf.0 * w), decoder)))
            }
        }
    }
}



impl<'a, T: 'a, TIndex> ToOperands<'a> for Funnel<'a, T, TIndex> {
    fn to_operands(self) -> Vec<(ChainingOperator, PeekableSeekable<Operand<'a>>)> {
        if self.result.is_empty() {
            return vec![];
        }
        vec![(self.chaining_operator,
              PeekableSeekable::new(Operand::Operated(Weight(1.0), self.result)))]
    }
}


/// END FUNNEL
#[derive(Debug, Copy, Clone)]
pub struct And;

impl And {
    pub fn next(operands: &mut [PeekableSeekable<Operand>]) -> Option<Posting> {
        let mut focus = try_option!(operands[0].next()); // Acts as temporary to be compared against
        let mut last_iter = 0; // The iterator that last set 'focus'
        'possible_documents: loop {
            // For every term
            for (i, input) in operands.iter_mut().enumerate() {
                // If the focus was set by the current iterator, we have a match
                // We cycled through all the iterators once
                if last_iter == i {
                    continue;
                }

                let v = try_option!(input.next_seek(&focus));
                if v.0 > focus.0 {
                    // If it is larger, we are now looking at a different focus.
                    // Reset focus and last_iter. Then start from the beginning
                    focus = v;
                    last_iter = i;
                    continue 'possible_documents;
                }
            }
            return Some(focus);
        }
    }

    pub fn next_seek(operands: &mut [PeekableSeekable<Operand>],
                     target: &Posting)
                     -> Option<Posting> {
        // Advance operands to `target`
        for op in operands.iter_mut() {
            op.peek_seek(target);
        }
        Self::next(operands)
    }

    pub fn progress(operands: &[PeekableSeekable<Operand>]) -> Progress {
        operands.iter()
            .map(|op| op.inner().progress())
            .max()
            .unwrap_or(Progress::done())
    }
}
