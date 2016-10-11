//! This module provides traits that are needed to create objects with certain
//! assumptions.
//!
//! `Persistent` provides methods to create and load `Persistent` objects with
//! the specification of a `Path`
//!
//! `Volatile` on the other hand provides only an empty constructor

use std::path::Path;

use storage::Result;

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
