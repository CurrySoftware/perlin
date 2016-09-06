use std;
use std::io::{Read, Write};


pub mod storage;
pub mod boolean_index;

/// The central trait of perlin. Indices tend to differ alot in implementation details
/// yet they all share this very basic interface
pub trait Index<'a, TTerm> {
    type Query;
    type QueryResult;

    fn execute_query(&'a self, query: &Self::Query) -> Self::QueryResult;
}

/// Defines API calls for reading/writing an index from/to binary.
/// Can be used for example to persist an Index as a file or send it as `TcpStream`.
// Unhappy with the name...
// TODO: Find a better one
// TODO: Is this necessary? Or just throw it away?
pub trait TransferableIndex
    where Self: Sized
{
    /// Writes the index as byte to the specified target.
    /// Returns Error or the number of bytes written
    fn write_to<TTarget: Write>(&mut self, target: &mut TTarget) -> std::io::Result<usize>;

    /// Reads an index from the specified source.
    fn read_from<TSource: Read>(source: &mut TSource) -> std::result::Result<Self, String>;
}


