use std;
use std::error::Error;
use std::sync::Arc;

pub mod fs_storage;
pub mod ram_storage;

pub type Result<T> = std::result::Result<T, StorageError>;

#[derive(Debug)]
pub enum StorageError {
    KeyNotFound,
    ReadError(Option<std::io::Error>),
    WriteError(Option<std::io::Error>),
}

impl std::fmt::Display for StorageError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for StorageError {
    fn description(&self) -> &str {
        match *self {
            StorageError::KeyNotFound => "Key was not found in storage!",
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
pub trait Storage<T> where Self: Sync + Send {
    /// Tries to get a value for a given Id.
    /// Returns an Error if read fails or if id is unknown.
    fn get(&self, id: u64) -> Result<Arc<T>>;

    /// Tries to store a value with a given Id.
    /// Returns an Error if Write fails or if there is no more space.
    fn store(&mut self, id: u64, data: T) -> Result<()>;
}
