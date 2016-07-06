use std::io::Result;
use std::path::Path;

pub mod boolean_index;

pub trait Index<TTerm> {
    type Query;
    type QueryResult;
    
    fn new() -> Self;

    fn index_document<TDocIterator: Iterator<Item=TTerm>>(&mut self, document: TDocIterator) -> usize;

    fn execute_query(&self, query: &Self::Query) -> Self::QueryResult;
}

pub trait PersistentIndex {
    fn write_to_file(&self, path: &Path) -> Result<()>;

    fn read_from_file(path: &Path) -> Self;
}
