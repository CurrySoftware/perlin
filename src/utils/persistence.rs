use std::path::Path;


pub trait Persistence {
    fn new(path: &Path) -> Self;
    fn load(path: &Path) -> Self;
}
