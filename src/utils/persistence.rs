use std::path::Path;

pub trait Persistent {
    fn create(path: &Path) -> Self;
    fn load(path: &Path) -> Self;
}

pub trait Volatile {
    fn new() -> Self;
}

