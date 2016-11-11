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

pub trait SeekingIterator {
    type Item;
    // TODO: Define how peek_seek and next_seek should work in terms of iterator progression
    // Especially in combination with OwningIterator::next, OwningIterator::peek and
    // QueryResultIterator::peek implementations
    // For now. Assume next_seek is not called after peek and peek advances the iterator so
    // that assert_eq!(it.peek_seek(t); it.next(), it.next_seek(t))
    fn next_seek(&mut self, &Self::Item) -> Option<Self::Item>;
    fn peek_seek(&mut self, &Self::Item) -> Option<&Self::Item>;
}
