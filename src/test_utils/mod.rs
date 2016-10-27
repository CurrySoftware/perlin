use std::env::temp_dir;
use std::path::PathBuf;

const TEST_FOLDER: &'static str = "perlin_tests";

/// Returns the `Path` to a folder where tests can do their worst
pub fn test_folder() -> PathBuf{
    temp_dir().join(TEST_FOLDER)
}
