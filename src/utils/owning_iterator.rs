//! This module provides the trait `OwningIterator`.
//!
//! An `OwningIterator`, as the name suggests, can both iterate over and own a
//! collection.
//!
//! The difference to for example `std::vec::IntoIter` is, that it returns
//! references rather than values.
//! This is achieved by changing the interface of the `next` function to
//! include the lifetime of the `OwningIterator` and making it non mutable.
//! Implementors of this trait need to either use interior mutability (e.g.
//! `Cell` or `RefCell`) or rely themselves on `OwningIterator`s.
//!
//! `OwningIterator`s are used for example in
//! `perlin::index::boolean_index::query_result_iterator::QueryResultIterator` .

use std::sync::Arc;
use std::cell::Cell;

/// Defines an interface for iterators that can
///
/// 1. Own a collection
///
/// 2. Hand out references to it that have the same lifetime as the iterator
/// itself
///
/// 3. And peek
pub trait OwningIterator<'a> {
    type Item;
    fn next(&'a self) -> Option<Self::Item>;
    fn peek(&'a self) -> Option<Self::Item>;
    fn len(&self) -> usize;
    
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub trait SeekingIterator<'a> {
    type Item;
    // TODO: Define how peek_seek and next_seek should work in terms of iterator progression
    // Especially in combination with OwningIterator::next, OwningIterator::peek and
    // QueryResultIterator::peek implementations
    // For now. Assume next_seek is not called after peek and peek advances the iterator so
    // that assert_eq!(it.peek_seek(t); it.next(), it.next_seek(t))
    fn next_seek(&'a self, Self::Item) -> Option<Self::Item>;
    fn peek_seek(&'a self, Self::Item) -> Option<Self::Item>;
}

/// Implements the `OwningIterator` trait for atomic reference counted `Vec`s
pub struct ArcIter<T> {
    data: Arc<Vec<T>>,
    pos: Cell<usize>,
}

impl<'a, T: 'a> OwningIterator<'a> for ArcIter<T> {
    type Item = &'a T;

    fn next(&'a self) -> Option<Self::Item> {
        if self.pos.get() < self.data.len() {
            self.pos.set(self.pos.get() + 1);
            return Some(&self.data[self.pos.get() - 1]);
        }
        None
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn peek(&'a self) -> Option<Self::Item> {
        if self.pos.get() >= self.len() {
            None
        } else {
            Some(&self.data[self.pos.get()])
        }
    }

    fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

impl<'a, T: 'a + Ord> SeekingIterator<'a> for ArcIter<T> {
    type Item = &'a T;

    fn next_seek(&'a self, target: Self::Item) -> Option<Self::Item> {
        let index = match self.data[self.pos.get()..].binary_search(target) {
            Ok(i) => self.pos.get() + i,
            Err(i) => self.pos.get() + i 
        };
        if index >= self.data.len()  {
            None
        } else {
            self.pos.set(index + 1);
            Some(&self.data[index])
        }
    }


    fn peek_seek(&'a self, target: Self::Item) -> Option<Self::Item> {
        let index = match self.data[self.pos.get()..].binary_search(target) {
            Ok(i) => self.pos.get() + i,
            Err(i) => self.pos.get() + i 
        };
        if index >= self.data.len()  {
            None
        } else {
            self.pos.set(index);
            Some(&self.data[index])
        }
    }
}


impl<T> ArcIter<T> {
    pub fn new(data: Arc<Vec<T>>) -> Self {
        ArcIter {
            data: data,
            pos: Cell::new(0),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    
    #[test]
    fn seeking_basic() {
        let data = Arc::new(vec![0,1,2,3,4,5]);
        let iter = ArcIter::new(data);

        assert_eq!(iter.next_seek(&0), Some(&0));
        assert_eq!(iter.next(), Some(&1));        
        assert_eq!(iter.next_seek(&4), Some(&4));
        assert_eq!(iter.next_seek(&3), Some(&5));
        assert_eq!(iter.next_seek(&5), None);
        assert_eq!(iter.next(), None);       
    }

    #[test]
    fn seeking_holes() {
        let data = Arc::new(vec![0,1,2,3,4,5,11,276,345,1024,5409,10004]);
        let iter = ArcIter::new(data);

        assert_eq!(iter.next_seek(&0), Some(&0));
        assert_eq!(iter.next(), Some(&1));        
        assert_eq!(iter.next_seek(&9), Some(&11));
        assert_eq!(iter.next_seek(&276), Some(&276));
        assert_eq!(iter.next_seek(&1025), Some(&5409));
        assert_eq!(iter.next(), Some(&10004));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn peek_seeking() {
        let data = Arc::new(vec![0,1,2,3,4,5]);
        let iter = ArcIter::new(data);

        assert_eq!(iter.peek_seek(&4), Some(&4));
        assert_eq!(iter.next(), Some(&4));
        assert_eq!(iter.peek_seek(&3), Some(&5));
        assert_eq!(iter.next(), Some(&5));
        assert_eq!(iter.next(), None);
    }        
}
