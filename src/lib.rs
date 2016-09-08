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
//! use perlin::index::storage::ram_storage::RamStorage;
//! use perlin::index::boolean_index::IndexBuilder;
//!
//! //Use `IndexBuilder` to construct a new Index and add documents to it.
//! let index = IndexBuilder::<_, RamStorage<_>>::new().create(vec![(0..10),
//! (0..15), (10..34)].into_iter());
//!
//! ```
//!

#[macro_use]
pub mod utils;
pub mod index;
