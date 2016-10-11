//! This module provides traits that are needed to create objects with certain
//! assumptions.
//!
//! `Persistent` provides methods to create and load `Persistent` objects with
//! the specification of a `Path`
//!
//! `Volatile` on the other hand provides only an empty constructor

use std::path::Path;

// TODO: These methods need to return Results.
// They can both fail (relatively likely actually)
pub trait Persistent {
    fn create(path: &Path) -> Self;
    fn load(path: &Path) -> Self;
    fn associated_files() -> &'static [&'static str]; 
}

pub trait Volatile {
    fn new() -> Self;
}
