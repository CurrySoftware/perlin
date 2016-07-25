use std::io::{Read, Result, Write};
use std;

pub mod boolean_index;

/// The central trait of perlin. Indices tend to differ alot in implementation details
/// yet they all share this basic interface
pub trait Index<'a, TTerm> {
    type Query;
    type QueryResult;
    
    fn new() -> Self;

    fn index_documents<TDocIterator: Iterator<Item=TTerm>>(&mut self, documents: Vec<TDocIterator>) -> Vec<u64>;

    fn execute_query(&'a self, query: &Self::Query) -> Self::QueryResult;
}

pub trait Provider<T>{
    fn get<'a>(&'a self, id: u64) -> Option<&'a T>;
    fn store(&mut self, id: u64, data: T);
}

/// Defines API calls for writing and reading an index from/to binary
/// Can be used for example to persist an Index as a file or send it as `TcpStream`.
pub trait PersistentIndex where Self : Sized {
    
    /// Writes the index as byte to the specified target.
    /// Returns Error or the number of bytes written
    fn write_to<TTarget: Write>(&self, target: &mut TTarget) -> Result<usize>;

    /// Reads an index from the specified source.
    fn read_from<TSource: Read>(source: &mut TSource) -> std::result::Result<Self, String>;
}


pub trait ByteEncodable {
    fn encode(&self) -> Vec<u8>;
}

pub trait ByteDecodable where Self: Sized {
    fn decode(Vec<u8>) -> std::result::Result<Self, String>;
}

