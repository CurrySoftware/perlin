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
//! use perlin::index::Index;
//! use perlin::index::boolean_index::BooleanIndex;
//! // Create a new index:
//! let mut index = BooleanIndex::new();
//! index.index_document(0..10);
//! index.index_document(5..15);
//! ```
//!




pub mod index;
