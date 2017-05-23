use std::hash::Hash;
use std::fmt::Debug;
use std::borrow::Borrow;
use std::marker::PhantomData;

use perlin_core::index::posting::{Posting, PostingIterator};
use perlin_core::utils::seeking_iterator::{PeekableSeekable, SeekingIterator};
use perlin_core::utils::progress::Progress;
use perlin_core::index::Index;

use language::CanApply;
use query::{ToOperands, Operand, Operator, ChainingOperator};

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

pub struct SplitFunnel<'a, T: 'a + Hash + Eq, CB> {
    index: &'a Index<T>,
    combinator: Combinator,
    chaining_operator: ChainingOperator,
    result: Vec<PeekableSeekable<Operand<'a>>>,
    callback: CB,
}

impl<'a, T: 'a + Hash + Eq, CB> SplitFunnel<'a, T, CB> {
    pub fn create(chaining_operator: ChainingOperator,
                  combinator: Combinator,
                  index: &'a Index<T>,
                  cb: CB)
                  -> Self {
        SplitFunnel {
            index: index,
            combinator: combinator,
            chaining_operator: chaining_operator,
            callback: cb,
            result: Vec::new(),
        }
    }
}

impl<'a, T: 'a + Hash + Eq + Ord, CB> CanApply<T> for SplitFunnel<'a, T, CB>
    where CB: CanApply<T>
{
    type Output = CB::Output;

    fn apply(&mut self, term: T) {
        // Query index
        match self.index.query_atom(&term) {
            PostingIterator::Empty => self.result.push(PeekableSeekable::new(Operand::Empty)),
            PostingIterator::Decoder(decoder) => {
                self.result.push(PeekableSeekable::new(Operand::Term(decoder)))
            }
        }
        // And keep going
        self.callback.apply(term);
    }
}


impl<'a, T: 'a + Hash + Eq, CB> ToOperands<'a> for SplitFunnel<'a, T, CB>
    where CB: ToOperands<'a>
{
    fn to_operands(self) -> Vec<(ChainingOperator, PeekableSeekable<Operand<'a>>)> {
        let mut other = self.callback.to_operands();
        match self.combinator {
            Combinator::All => {
                other.push((self.chaining_operator,
                            PeekableSeekable::new(Operand::Operated(Box::new(And {}), self.result))))
            }
            Combinator::Any => {
                other.push((self.chaining_operator,
                            PeekableSeekable::new(Operand::Operated(Box::new(Or {}), self.result))))
            }
        }
        other
    }
}

/// This funnel is used at an end of a query pipeline
/// It calls `index.query_atom` and stores the result, which is lazy
/// When `to_operand` is then called, it packs everything into an operator!
// TODO: In this struct lies an opportunity to optimize
// If the operator is AND and one term returns an empty posting iterator
// We could skip the rest
// If the operator is Or and one term returns an empty posting iterator we
// could discard it
pub struct Funnel<'a, T:'a, TIndex: 'a> {
    index: &'a TIndex,
    combinator: Combinator,
    chaining_operator: ChainingOperator,
    result: Vec<PeekableSeekable<Operand<'a>>>,
    _term: PhantomData<T>
}

impl<'a, T: 'a, TIndex: 'a> Funnel<'a, T, TIndex> {
    pub fn create(chaining_operator: ChainingOperator,
                  combinator: Combinator,
                  index: &'a TIndex)
                  -> Self {
        Funnel {
            index: index,
            combinator: combinator,
            chaining_operator: chaining_operator,
            result: Vec::new(),
            _term: PhantomData,
        }
    }
}


impl<'a: 'b, 'b, T: 'a + Hash + Eq + Ord + Debug, TIndex> CanApply<&'b T> for Funnel<'a, T, TIndex>
    where TIndex: Borrow<Index<T>>
{
    type Output = T;

    fn apply(&mut self, term: &'b T) {
        match (self.index.borrow()).query_atom(&term) {
            PostingIterator::Empty => self.result.push(PeekableSeekable::new(Operand::Empty)),
            PostingIterator::Decoder(decoder) => {
                self.result.push(PeekableSeekable::new(Operand::Term(decoder)))
            }
        }
    }
}


impl<'a, T: 'a + Hash + Eq + Ord + Debug, TIndex> CanApply<T> for Funnel<'a, T, TIndex>
    where TIndex: Borrow<Index<T>>
{
    type Output = T;

    fn apply(&mut self, term: T) {
        match (self.index.borrow()).query_atom(&term) {
            PostingIterator::Empty => self.result.push(PeekableSeekable::new(Operand::Empty)),
            PostingIterator::Decoder(decoder) => {
                self.result.push(PeekableSeekable::new(Operand::Term(decoder)))
            }
        }
    }
}



impl<'a, T:'a,  TIndex> ToOperands<'a> for Funnel<'a, T, TIndex> {
    fn to_operands(self) -> Vec<(ChainingOperator, PeekableSeekable<Operand<'a>>)> {
        if self.result.is_empty() {
            return vec![];
        }
        match self.combinator {
            Combinator::All => {
                vec![(self.chaining_operator,
                      PeekableSeekable::new(Operand::Operated(Box::new(And {}), self.result)))]
            }
            Combinator::Any => {
                vec![(self.chaining_operator,
                      PeekableSeekable::new(Operand::Operated(Box::new(Or {}), self.result)))]
            }
        }

    }
}
/// END FUNNEL

#[derive(Debug, Copy, Clone)]
pub struct And;

impl Operator for And {
    fn next(&mut self, operands: &mut [PeekableSeekable<Operand>]) -> Option<Posting> {
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

    fn next_seek(&mut self,
                 operands: &mut [PeekableSeekable<Operand>],
                 target: &Posting)
                 -> Option<Posting> {
       //Advance operands to `target`
        for op in operands.iter_mut() {
            op.peek_seek(target);
        }
        self.next(operands)
    }

    fn progress(&self,
                operands: &[PeekableSeekable<Operand>]) -> Progress {
        operands.iter().map(|op| op.inner().progress()).max().unwrap_or(Progress::done())
    }
}

#[derive(Debug, Copy, Clone)]
pub struct Or;

impl Operator for Or {
    fn next(&mut self, operands: &mut [PeekableSeekable<Operand>]) -> Option<Posting> {
        // TODO: Probably improveable
        // Find the smallest current value of all operands
        let min_doc_id = operands.iter_mut()
            .map(|op| op.peek())
            .filter(|val| val.is_some())
            .map(|val| val.unwrap().doc_id())
            .min();

        // Walk over all operands. Advance those who emit the min value
        if min_doc_id.is_some() {
            let mut i = 0;
            let mut tmp = None;
            while i < operands.len() {
                let v = operands[i].peek().map(|p| p.doc_id());
                if !v.is_none() && v == min_doc_id {
                    tmp = operands[i].next();
                }
                i += 1;
            }
            tmp
        } else {
            None
        }
    }

    fn next_seek(&mut self,
                 operands: &mut [PeekableSeekable<Operand>],
                 target: &Posting)
                 -> Option<Posting> {
        //Advance operands to `target`
        for op in operands.iter_mut() {
            op.peek_seek(target);
        }
        self.next(operands)
    }

    fn progress(&self,
                operands: &[PeekableSeekable<Operand>]) -> Progress {
        operands.iter().map(|op| op.inner().progress()).min().unwrap_or(Progress::done())
    }
}
