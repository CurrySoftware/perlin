//! Perlin is a free and open source information retrieval library.
//!
//! Version 0.0.0 (not released)
//!
//! # Features
//!
//! # Basic Usage
//!
//! Add Perlin to Cargo.toml
//! ```toml
//! [dependencies]
//! perlin = "0.0"
//! ```
//! and import it in your crate root:
//!
//! ```rust
//! extern crate perlin;
//! ```
//!
//!
//! ## Build index and run queries
//! ```rust
//! use perlin::index::Index;
//! use perlin::storage::RamStorage;
//! use perlin::index::boolean_index::{QueryBuilder, IndexBuilder};
//!
//! //Use `IndexBuilder` to construct a new Index and add documents to it.
//! let index = IndexBuilder::<_, RamStorage<_>>::new().create(vec![(0..10),
//! (0..15), (10..34)].into_iter()).unwrap();
//!
//! //Now use the `QueryBuilder` to construct a query
//! //Simple query for the number 4
//! let simple_query = QueryBuilder::atom(4).build();
//!
//! //When executing queries the index does not evaluate return all results at
//! //once.
//! //Rather it runs lazily and returns an iterator over the resulting document
//! //ids.
//! for id in index.execute_query(&simple_query) {
//!    println!("{}", id); //Will print 0 and 1
//! }
//!
//! ```
//!
#![deny(missing_docs, warnings)]

#[macro_use]
mod utils;
pub mod language;
pub mod index;
pub mod storage;

