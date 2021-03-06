use std::hash::Hash;
use std::fmt::Debug;
use std::marker::PhantomData;

use perlin_core::index::posting::{Posting, PostingIterator, PostingDecoder};
use perlin_core::utils::seeking_iterator::{PeekableSeekable, SeekingIterator};
use perlin_core::utils::progress::Progress;

use language::CanApply;
use query::{Weight, ToOperands, Operand};
use field::{Field, Fields};

#[derive(Debug)]
pub enum Combinator {
    All,
    Any,
}

/// This funnel is used at an end of a query pipeline
/// It calls `index.query_atom` and stores the result, which is lazy
/// When `to_operand` is then called, it packs everything into an operator!
pub struct Funnel<'a, T: 'a, TIndex: 'a> {
    index: &'a TIndex,
    result: Vec<PeekableSeekable<Operand<'a>>>,
    _term: PhantomData<T>,
}

impl<'a, T: 'a, TIndex: 'a> Funnel<'a, T, TIndex> {
    pub fn create(index: &'a TIndex) -> Self {
        Funnel {
            index,
            result: Vec::new(),
            _term: PhantomData,
        }
    }

    fn add_posting_list(&mut self,
                        weight: Weight,
                        decoder: PostingDecoder<'a>,
                        term: String,
                        field: String) {
        if weight.0 > 0. {
            self.result.push(PeekableSeekable::new(Operand::Term(weight, decoder, term, field)));
        }
    }
}

impl<'a: 'b, 'b, T: 'a + Hash + Eq + Ord + Debug + ToString> CanApply<&'b T>
    for Funnel<'a, T, Fields<T>> {
    type Output = T;

    fn apply(&mut self, term: &'b T) {
        for (key, index) in self.index.fields.iter() {
            let w = 1./index.term_doc_ratio;
            match index.query_atom(&term) {
                (idf, PostingIterator::Decoder(decoder)) => {
                    self.add_posting_list(Weight(idf.0 * w),
                                          decoder,
                                          term.to_string(),
                                          key.clone());
                }
                _ => {}
            }
        }
    }
}


impl<'a, T: 'a + Hash + Eq + Ord + Debug + ToString> CanApply<T> for Funnel<'a, T, Fields<T>> {
    type Output = T;

    fn apply(&mut self, term: T) {
        for (key, index) in self.index.fields.iter() {
            let w = 1./index.term_doc_ratio;
            match index.query_atom(&term) {
                (idf, PostingIterator::Decoder(decoder)) => {
                    self.add_posting_list(Weight(idf.0 * w),
                                          decoder,
                                          term.to_string(),
                                          key.clone());
                }
                _ => {}
            }
        }
    }
}


impl<'a: 'b, 'b, T: 'a + Hash + Eq + Ord + Debug + ToString> CanApply<&'b T>
    for Funnel<'a, T, Field<T>> {
    type Output = T;

    fn apply(&mut self, term: &'b T) {
        let w = 1./self.index.term_doc_ratio;
        match self.index.query_atom(&term) {
            (idf, PostingIterator::Decoder(decoder)) => {
                self.add_posting_list(Weight(idf.0 * w),
                                      decoder,
                                      term.to_string(),
                                      self.index.name.clone());
            }
            _ => {}
        }
    }
}


impl<'a, T: 'a + Hash + Eq + Ord + Debug + ToString> CanApply<T> for Funnel<'a, T, Field<T>> {
    type Output = T;

    fn apply(&mut self, term: T) {
        let w = 1./self.index.term_doc_ratio;
        match self.index.query_atom(&term) {
            (idf, PostingIterator::Decoder(decoder)) => {
                self.add_posting_list(Weight(idf.0 * w),
                                      decoder,
                                      term.to_string(),
                                      self.index.name.clone());
            }
            _ => {}
        }
    }
}



impl<'a, T: 'a, TIndex> ToOperands<'a> for Funnel<'a, T, TIndex> {
    fn to_operands(self) -> Vec<PeekableSeekable<Operand<'a>>> {
        self.result
    }
}


/// END FUNNEL
#[derive(Debug, Copy, Clone)]
pub struct And;

impl And {
    pub fn next(operands: &mut [PeekableSeekable<Operand>]) -> Option<Posting> {
        if operands.is_empty() {
            return None;
        }
        let mut focus = operands[0].next()?; // Acts as temporary to be compared against
        let mut last_iter = 0; // The iterator that last set 'focus'
        'possible_documents: loop {
            // For every term
            for (i, input) in operands.iter_mut().enumerate() {
                // If the focus was set by the current iterator, we have a match
                // We cycled through all the iterators once
                if last_iter == i {
                    continue;
                }

                let v = input.next_seek(&focus)?;
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
