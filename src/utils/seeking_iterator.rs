//! This module provides the trait `SeekingIterator`.
//!
//! A `SeekingIterator`, as the name suggests, can seek to a certain position.
//!
//! `SeekingIterator`s are implemented for example in
//! `perlin::index::boolean_index::query_result_iterator::QueryResultIterator`
//! or `perlin::index::boolean_index::posting::PostingDecoder`

use std::option::Option;

/// Trait that defines an iterator type that allow seeking access.
/// This is especially usefull for query evaluation.
pub trait SeekingIterator {
    type Item;

    /// Yields an Item that is >= the passed argument or None if no such element exists
    fn next_seek(&mut self, &Self::Item) -> Option<Self::Item>;
}

/// Wraps an iterator and provides peeking abilities to it.
/// Very similar to `std::iter::Peekable`
pub struct PeekableSeekable<I: Iterator> {
    iter: I,
    peeked: Option<I::Item>,
}



impl<I> SeekingIterator for PeekableSeekable<I>
    where I: Iterator<Item = <I as SeekingIterator>::Item> + SeekingIterator,
          <I as SeekingIterator>::Item: Ord
{
    type Item = <I as SeekingIterator>::Item;

    #[inline]
    fn next_seek(&mut self, other: &Self::Item) -> Option<Self::Item> {
        //Check if a peeked value exists that matches. Otherwise just forward the request.
        let peeked = self.peeked.take();
        if peeked.is_some() {
            let val = peeked.unwrap();
            if val >= *other {
                return Some(val);
            }
        }
        self.iter.next_seek(other)
    }
}

impl<I> PeekableSeekable<I>
    where I: Iterator<Item = <I as SeekingIterator>::Item> + SeekingIterator
{
    pub fn new(iter: I) -> Self {
        PeekableSeekable {
            iter: iter,
            peeked: None,
        }
    }

    #[inline]
    pub fn peek(&mut self) -> Option<&<I as Iterator>::Item> {
        if self.peeked.is_none() {
            self.peeked = self.iter.next();
        }
        match self.peeked {
            Some(ref value) => Some(value),
            None => None,
        }
    }

    #[inline]
    pub fn peek_seek(&mut self, other: &<I as SeekingIterator>::Item) -> Option<&<I as Iterator>::Item> {
        if self.peeked.is_none() {
            self.peeked = self.iter.next_seek(other);
        }
        match self.peeked {
            Some(ref value) => Some(value),
            None => None,
        }
    }

    #[inline]
    pub fn inner(&self) -> &I {
        &self.iter
    }
}

// Heavily "inspired" by `std::iter::Peekable`
impl<I> Iterator for PeekableSeekable<I>
    where I: Iterator<Item = <I as SeekingIterator>::Item> + SeekingIterator
{
    type Item = <I as Iterator>::Item;

    #[inline]
    fn next(&mut self) -> Option<<I as Iterator>::Item> {
        match self.peeked {
            Some(_) => self.peeked.take(),
            None => self.iter.next(),
        }
    }


    #[inline]
    fn count(self) -> usize {
        (if self.peeked.is_some() { 1 } else { 0 }) + self.iter.count()
    }

    #[inline]
    fn nth(&mut self, n: usize) -> Option<<I as Iterator>::Item> {
        match self.peeked {
            Some(_) if n == 0 => self.peeked.take(),
            Some(_) => {
                self.peeked = None;
                self.iter.nth(n - 1)
            }
            None => self.iter.nth(n),
        }
    }

    #[inline]
    fn last(self) -> Option<<I as Iterator>::Item> {
        self.iter.last().or(self.peeked)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        let (lo, hi) = self.iter.size_hint();
        if self.peeked.is_some() {
            let lo = lo.saturating_add(1);
            let hi = hi.and_then(|x| x.checked_add(1));
            (lo, hi)
        } else {
            (lo, hi)
        }
    }
}
