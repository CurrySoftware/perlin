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
//! ## Indexing documents and run queries
//! ```rust
//! use std::path::Path;
//! use perlin::index::Index;
//! use perlin::index::storage::ram_storage::RamStorage;
//! use perlin::index::boolean_index::{BooleanIndex};

//! //Create a new index and tell it where to store its data.
//! //In this case we will tell it to store its data in memory
//! let mut index = BooleanIndex::new(RamStorage::new());
//! index.index_documents(vec![(0..10), (0..15), (10..34)]);
//! ```
//!

#[macro_use]
pub mod utils;
pub mod index;
