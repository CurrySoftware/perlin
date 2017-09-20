use std::cmp::Ordering;
use std::fmt;

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

#[derive(Debug, Copy, Clone, PartialOrd, PartialEq)]
pub struct Weight(pub f32);

impl Eq for Weight {}

impl Ord for Weight {
    fn cmp(&self, other: &Weight) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Less)
    }
}

pub struct WeightingOperator<'a> {
    max_weight: Weight,
    already_emitted: Vec<Posting>,
    filters: Vec<PeekableSeekable<Operand<'a>>>,
    operands: Vec<PeekableSeekable<Operand<'a>>>,
    current_operands: Option<Vec<PeekableSeekable<Operand<'a>>>>,
    counter: usize,
}

impl<'a> Iterator for WeightingOperator<'a> {
    type Item = Posting;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let next = self.internal_next();
            if next.is_none() {
                return next;
            }

            let posting = next.unwrap();

            let index = match self.already_emitted.binary_search(&posting) {
                Ok(_) => {
                    // We already emitted that posting
                    continue;
                }
                Err(index) => {
                    // New posting HUZAA!
                    index
                }
            };
            self.already_emitted.insert(index, posting.clone());
            return Some(posting);
        }
    }
}

impl<'a> WeightingOperator<'a> {
    /// Operands are ordered by weight.
    /// For example
    /// Op[0] = "hans in body" | Op[1] = "hans in tag" | Op[2] = "hans in title"
    ///
    /// This operator returns
    /// Every results of Op[0] && Op[1] && Op[2]
    /// Next: All results of Op[1] && Op[2] // Minus previous results
    /// Next: All results of Op[0] && Op[2] // Minus previous results
    /// Next: All results of Op[2] // Minus previous results
    /// and so on...
    ///
    /// If you paid attention you noticed the pattern:
    ///
    /// | Step | Op[0] | Op[1] | Op[2] |
    /// |------+-------+-------+-------|
    /// |    0 |     1 |     1 |     1 |
    /// |    1 |     0 |     1 |     1 |
    /// |    2 |     1 |     0 |     1 |
    /// |    3 |     0 |     0 |     1 |
    /// |    4 |     1 |     1 |     0 |
    /// |    5 |     0 |     1 |     0 |
    /// |    6 |     1 |     0 |     0 |
    /// |    7 |     0 |     0 |     0 |
    ///
    /// This ensures, that the most relevant results are yielded first
    ///
    /// You might be worried about runtime and complexity
    /// The number of operands is query terms * fields
    /// And the number of steps is 2^n
    ///
    /// BUT: Perlin is designed for human consumption of search results
    /// That means: in all likelihood we will need to yield 10 results
    /// The 99% percentile is probably below 100 results
    /// So dont worry... (hopefully I'm right)
    fn internal_next(&mut self) -> Option<Posting> {
        loop {
            // If we have steps left
            if let Some(mut current_operands) = self.current_operands.take() {
                // NOTE: This is filthy fix as soon as nonliteral borrowing lands
                // Get next entry from step
                let next = And::next(&mut current_operands);
                if next.is_none() {
                    // If it is none... we need to go to the next step
                    if self.counter < (2 as usize).pow(self.operands.len() as u32) {
                        // Get all the relevant operands + filters!
                        let mut new_current_operands = self.filters.clone();
                        let mut curr_weight = Weight(0.);
                        for (i, op) in self.operands.iter().enumerate() {
                            let pow = (2 as usize).pow(i as u32);
                            if pow & self.counter == 0 {
                                curr_weight = Weight(curr_weight.0 + op.inner().weight().0);
                                new_current_operands.push(op.clone());
                            }
                        }

                        if new_current_operands.is_empty() || (curr_weight.0 < (0.01 * self.max_weight.0))  {
                            return None;
                        }
                        // TODO: Sort current operands by length(!)
                        self.counter += 1;
                        println!("NEW OPERANDS: {:?}", new_current_operands); 
                        self.current_operands = Some(new_current_operands);
                        continue;
                    } else {
                        // We are done!
                        self.current_operands = None;
                        return None;
                    }
                } else {
                    self.current_operands = Some(current_operands);
                    return next;
                }
            } else {
                return None;
            }
        }
    }

    // TODO: Think about something more correct(!)
    pub fn progress(&self) -> Progress {
        if let Some(ref operands) = self.current_operands {
            operands.iter()
                .map(|op| op.inner().progress())
                .max()
                .unwrap_or(Progress::done())
        } else {
            println!("Progress: DONE!");
            Progress::done()
        }
    }

    pub fn create(mut operands: Vec<PeekableSeekable<Operand<'a>>>,
                  filters: Vec<PeekableSeekable<Operand<'a>>>)
                  -> Self {
        println!("WeightedOperator with operands {:?} and filters {:?}", operands, filters);
        operands.sort_by_key(|op| op.inner().weight());
        let mut current_operands = operands.clone();
        current_operands.append(&mut filters.clone());
        let max_weight = operands.iter().fold(Weight(0.), |acc, ref op| Weight(acc.0 + op.inner().weight().0));
        WeightingOperator {
            already_emitted: Vec::new(),
            max_weight,
            filters,
            operands,
            current_operands: Some(current_operands),
            // Step 0 is set up in the lines before
            counter: 1,
        }

    }
}

#[derive(Clone)]
pub enum Operand<'a> {
    Term(Weight, PostingDecoder<'a>, String, String),
}

impl<'a> fmt::Debug for Operand<'a> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Operand::Term(weight, _, ref term, ref field) => {
                write!(f,
                       "Querying term {:?} on field {:?} with weight {:?}",
                       term,
                       field,
                       weight)
            }
        }
    }
}

impl<'a> Iterator for Operand<'a> {
    type Item = Posting;

    fn next(&mut self) -> Option<Posting> {
        match *self {
            Operand::Term(_, ref mut decoder, _, _) => decoder.next(),
        }
    }
}

impl<'a> SeekingIterator for Operand<'a> {
    type Item = Posting;

    fn next_seek(&mut self, other: &Posting) -> Option<Posting> {
        match *self {
            Operand::Term(_, ref mut decoder, _, _) => decoder.next_seek(other),
        }
    }
}

impl<'a> Operand<'a> {
    pub fn weight(&self) -> Weight {
        match *self {
            Operand::Term(w, _, _, _) => w,
        }
    }

    pub fn progress(&self) -> Progress {
        match *self {
            Operand::Term(_, ref decoder, _, _) => decoder.progress(),
        }
    }
}

pub trait ToOperands<'a> {
    fn to_operands(self) -> Vec<PeekableSeekable<Operand<'a>>>;
}

#[derive(Clone)]
pub struct Query<'a> {
    pub query: &'a str,
    pub filter: Vec<PeekableSeekable<Operand<'a>>>,
}

impl<'a> Query<'a> {
    pub fn new(query: &'a str) -> Self {
        Query {
            query: query,
            filter: vec![],
        }
    }

    pub fn filter_by(mut self, filter: PostingIterator<'a>) -> Self {
        match filter {
            PostingIterator::Decoder(decoder) => {
                self.filter.push(PeekableSeekable::new(Operand::Term(Weight(1.0),
                                                                     decoder,
                                                                     "filter term".to_string(),
                                                                     "filter field".to_string())))
            }
            _ => {}
        }
        self
    }
}
