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
//! use perlin::index::boolean_index::{BooleanIndex, RamPostingProvider, FsPostingProvider};

//! //Create a new index and tell it where to store its data.
//! //In this case we will tell it to store its data in memory
//! let mut index = BooleanIndex::new(Box::new(RamPostingProvider::new()));
//! //If you want it to store data on disk, because you have limited RAM or lots of data
//! //use FsPostingProvider like
//! let mut fs_index: BooleanIndex<usize> = BooleanIndex::new(
//!                           Box::new(FsPostingProvider::new(Path::new("/tmp/index.bin"))));
//! index.index_documents(vec![(0..10), (0..15), (10..34)]);
//! ```
//!


pub mod index;
