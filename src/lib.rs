//! perlin is a free and open source search engine library build on top of perlin-core
//!
//! It aims to be fast for typical human consumption of search results.

extern crate perlin_core;

mod document;
mod perlin_index;

pub use document::Document;
pub use perlin_index::PerlinIndex;

#[cfg(test)]
pub mod test_utils;
