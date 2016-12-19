use std::fs;
use std::env::temp_dir;
use std::path::PathBuf;

const TEST_FOLDER: &'static str = "perlin_tests";

/// Returns the `Path` to a folder where tests can do their worst
pub fn test_dir() -> PathBuf{
    temp_dir().join(TEST_FOLDER)
}

pub fn create_test_dir(dir: &str) -> PathBuf {
    let path = test_dir().join(dir);
    fs::create_dir_all(&path).unwrap();
    path
}
