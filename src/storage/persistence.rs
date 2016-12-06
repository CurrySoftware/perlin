//! This module provides traits that are needed to create objects with certain
//! assumptions.
//!
//! `Persistent` provides methods to create and load `Persistent` objects with
//! the specification of a `Path`
//!
//! `Volatile` on the other hand provides only an empty constructor

use std::io;
use std::fmt;
use std::result;
use std::path::Path;
use std::error::Error;

pub type Result<T> = result::Result<T, PersistenceError>;


pub trait Persistent
    where Self: Sized
{
    fn create(path: &Path) -> Result<Self>;
    fn load(path: &Path) -> Result<Self>;
    fn associated_files() -> &'static [&'static str];
}

pub trait Volatile {
    fn new() -> Self;
}

#[derive(Debug)]
/// Error kinds that can occur during persistence operations
pub enum PersistenceError {
    /// No path was specified where to load/store data
    PersistPathNotSpecified,
    /// Some required files are missing
    MissingFiles(Vec<&'static str>),
    /// Attempted to load from a file, not a directory
    PersistPathIsFile,
    /// We encountered corrupt data
    CorruptData(Option<&'static str>),
    /// An error occured during an IO operation
    IO(io::Error),
}
impl PersistenceError {
    /// Small helper function
    /// Expects a path and a number of expected files and returns the files which are not in that path!
    pub fn missing_files(path: &Path, expected_files: &[&'static str]) -> Result<()> {
        let files = expected_files.iter().map(|f| *f).filter(|f| !path.join(f).exists()).collect::<Vec<_>>();
        if files.is_empty() {
            return Ok(());
        }
        Err(PersistenceError::MissingFiles(files))
    }
}

impl From<io::Error> for PersistenceError {
    fn from(err: io::Error) -> Self {
        PersistenceError::IO(err)
    }
}

impl fmt::Display for PersistenceError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl Error for PersistenceError {
    fn description(&self) -> &str {
        match *self {
            PersistenceError::PersistPathNotSpecified => "No path was specified!",
            PersistenceError::MissingFiles(_) => "Some files are missing!",
            PersistenceError::PersistPathIsFile => "Expected a directory but got a File!",
            PersistenceError::CorruptData(_) => "Corrupt data was loaded!",
            PersistenceError::IO(_) => "Error occured during IO-Operation!",
        }
    }

    fn cause(&self) -> Option<&Error> {
        None
    }
}
