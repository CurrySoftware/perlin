//! perlin is a free and open source search engine library build on top of
//! perlin-core
//!
//! It aims to be fast for typical human consumption of search results (e.g.
//! ten at a time).
//!
//! The previously released version 0.1 differs massively from this. Please
//! refer to tag 'v0.1' for the code  and
//! [https://doc.perlin-ir.org/v0.1/perlin/index.html] for documentation

extern crate perlin_core;
extern crate rust_stemmers;

#[macro_use]
pub mod language;
pub mod document_index;
pub mod document;
pub mod field;

pub use document_index::DocumentIndex;

#[cfg(test)]
pub mod test_utils;
