//! Provides everything needed for building and querying indices.
//!
//! Structs that implement the `Index` trait are made public here.
//! Currently only `BooleanIndex` falls in that category.
//!
//! Indices tend to hold large and complex data structures. To allow a flexible
//! usage
//! there are different implementations of storing them (e.g. on disk, in ram).
//! These storage implementations can be found in `storage`.
//!
//! Please refer to [`IndexBuilder`](boolean_index/struct.IndexBuilder.html) for usage details.
//!

pub mod boolean_index;

/// The central trait of perlin. Indices tend to differ alot in implementation
/// details.
///
/// yet they all share this very basic interface.
pub trait Index<'a, TTerm> {
    /// Specifies the query-object type to be used with an `Index`
    type Query;
    /// Specifies the result returned from the `execute_query()`-method
    type QueryResult;

    /// Runs a query and returns all matching documents
    fn execute_query(&'a self, query: &Self::Query) -> Self::QueryResult;
}
