use std;
use std::error::Error;
use std::sync::Arc;

pub mod fs_storage;
pub mod ram_storage;

pub type Result<T> = std::result::Result<T, StorageError>;

#[derive(Debug)]
pub enum StorageError{
    KeyNotFound,
    OutOfSpace(Option<Box<Error>>),
    WriteError(Option<Box<Error>>),
    ReadError(Option<Box<Error>>)
}


/// Defines a common interface between multiple storage types
/// The index uses them to store data like the posting lists
pub trait Storage<T> : Sync {
    
    /// Tries to get a value for a given Id.
    /// Returns an Error if read fails or if id is unknown.
    fn get(&self, id: u64) -> Result<Arc<T>>;

    /// Tries to store a value with a given Id.
    /// Returns an Error if Write fails or if there is no more space.
    fn store(&mut self, id: u64, data: T) -> Result<()>;
}
