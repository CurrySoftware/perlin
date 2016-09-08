pub mod storage;
pub mod boolean_index;

/// The central trait of perlin. Indices tend to differ alot in implementation
/// details
/// yet they all share this very basic interface
pub trait Index<'a, TTerm> {
    type Query;
    type QueryResult;

    fn execute_query(&'a self, query: &Self::Query) -> Self::QueryResult;
}
