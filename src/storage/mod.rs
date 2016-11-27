//! This module contains the trait `Storage` and implementations of it.
//!
//! `Storage`s are used by implementators of the `Index` trait to store and
//! retrieve their sometimes complex and huge datastructures
//! For small collections, a `RamStorage` will suffice. If the collections are
//! larger than the size of RAM though, a different solution is needed.
//!
//! To enable flexibility and perhaps even use case specific user
//! implementations, this trait serves as an interface for Indices to be use.
//! Current implementations are `RamStorage` for smaller collections that fit
//! completely in RAM and `FsStorage` which writes and reads data from disk and
//! thus allows the handling of much larger collections.
use std;
use std::io;
use std::fmt;
use std::error::Error;
use std::sync::Arc;

pub use storage::fs_storage::FsStorage;
pub use storage::ram_storage::RamStorage;
pub use storage::byte_code::{ByteDecodable, ByteEncodable, DecodeError, DecodeResult};

pub mod compression;
mod fs_storage;
mod ram_storage;
mod byte_code;



/// Aliases Result<T, `StorageError`> to Result<T> for readability and maintainability
pub type Result<T> = std::result::Result<T, StorageError>;

#[derive(Debug)]
/// Errors that can occur during retrieval or storage of a value
pub enum StorageError {
    /// The key which should be retrieved could not be found
    KeyNotFound,
    /// General IO error
    IO(io::Error),
    /// Error occured during read operation
    ReadError(Option<io::Error>),
    /// Error occured during write operation
    WriteError(Option<io::Error>),
}



impl fmt::Display for StorageError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl From<io::Error> for StorageError {
    fn from(err: io::Error) -> Self {
        StorageError::IO(err)
    }
}

impl From<StorageError> for io::Error {
    fn from(err: StorageError) -> io::Error {
        return match err {
            StorageError::IO(e) => e,
            StorageError::ReadError(Some(e)) => e,
            StorageError::WriteError(Some(e)) => e,
            _ => io::Error::new(io::ErrorKind::Other, format!("{}", err))
        }
    }
}

impl Error for StorageError {
    fn description(&self) -> &str {
        match *self {
            StorageError::KeyNotFound => "Key was not found in storage!",
            StorageError::IO(_) => "An error occured during an IO-operation!",
            StorageError::ReadError(_) => "An error occured while trying to read from storage!",
            StorageError::WriteError(_) => "An error occured while trying to write to storage!",
        }
    }

    fn cause(&self) -> Option<&Error> {
        match *self {
            StorageError::ReadError(Some(ref cause)) |
            StorageError::WriteError(Some(ref cause)) => Some(cause),
            _ => None,
        }
    }
}

/// Defines a common interface between multiple storage types
/// The index uses them to store data like the posting lists
// TODO: Needs methods to delete and/or update items
pub trait Storage<T>
    where Self: Sync + Send
{
    /// Tries to get a value for a given Id.
    /// Returns an Error if read fails or if id is unknown.
    fn get(&self, id: u64) -> Result<Arc<T>>;

    /// Tries to store a value with a given Id.
    /// Returns an Error if Write fails or if there is no more space.
    fn store(&mut self, id: u64, data: T) -> Result<()>;

    /// Returns the number of stored elements
    fn len(&self) -> usize;

    /// Returns true if storage is empty
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}
