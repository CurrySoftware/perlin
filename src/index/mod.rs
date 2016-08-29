use std::io::{Read, Result, Write};
use std;

pub mod storage;
pub mod boolean_index;

/// The central trait of perlin. Indices tend to differ alot in implementation details
/// yet they all share this basic interface
pub trait Index<'a, TTerm> {
    type Query;
    type QueryResult;

    fn index_documents<TDocIterator: Iterator<Item=TTerm>>(&mut self, documents: Vec<TDocIterator>) -> Vec<u64>;

    fn execute_query(&'a self, query: &Self::Query) -> Self::QueryResult;
}

/// Defines API calls for reading/writing an index from/to binary.
/// Can be used for example to persist an Index as a file or send it as `TcpStream`.
//Unhappy with the name...
//TODO: Find a better one
pub trait TransferableIndex where Self : Sized {
    
    /// Writes the index as byte to the specified target.
    /// Returns Error or the number of bytes written
    fn write_to<TTarget: Write>(&mut self, target: &mut TTarget) -> Result<usize>;

    /// Reads an index from the specified source.
    fn read_from<TSource: Read>(source: &mut TSource) -> std::result::Result<Self, String>;
}



