//! This module provides the trait `OwningIterator`.
//!
//! An `OwningIterator`, as the name suggests, can both iterate over and own a collection.
//!
//! The difference to for example `std::vec::IntoIter` is, that it returns references rather than values.
//! This is achieved by changing the interface of the `next` function to include the lifetime of the `OwningIterator` and making it non mutable.
//! Implementors of this trait need to either use interior mutability (e.g. `Cell` or `RefCell`) or rely themselves on `OwningIterator`s.
//!
//! `OwningIterator`s are used for example in `perlin::index::boolean_index::query_result_iterator::QueryResultIterator` .

use std::sync::Arc;
use std::cell::Cell;

/// Defines an interface for iterators that can
///
/// 1. Own a collection
///
/// 2. Hand out references to it that have the same lifetime as the iterator itself
///
/// 3. And peek 
pub trait OwningIterator<'a> {
    type Item;
    fn next(&'a self) -> Option<Self::Item>;
    fn peek(&'a self) -> Option<Self::Item>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool;
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


impl<T> ArcIter<T>{
    pub fn new(data: Arc<Vec<T>>) -> Self {
        ArcIter {
            data: data,
            pos: Cell::new(0)
        }
    }
}
