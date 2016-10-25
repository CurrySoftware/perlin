use std::fs;
use std::marker::PhantomData;
use std::path::{PathBuf, Path};
use std::hash::Hash;

use storage::Storage;
use storage::{ByteEncodable, ByteDecodable};
use storage::chunked_storage::IndexingChunk;
use utils::persistence::{Volatile, Persistent};


use index::boolean_index;
use index::boolean_index::{Result, Error, BooleanIndex};
use index::boolean_index::posting::Listing;

const REQUIRED_FILES: [&'static str; 2] = [boolean_index::VOCAB_FILENAME, boolean_index::STATISTICS_FILENAME];

/// `IndexBuilder` is used to build `BooleanIndex` instances
pub struct IndexBuilder<TTerm, TStorage> {
    persistence: Option<PathBuf>,
    _storage: PhantomData<TStorage>,
    _term: PhantomData<TTerm>,
}

impl<TTerm, TStorage> IndexBuilder<TTerm, TStorage>
    where TTerm: Ord,
          TStorage: Storage<Listing>
{
    /// Creates a new instance of `IndexBuilder`
    pub fn new() -> Self {
        IndexBuilder {
            persistence: None,
            _storage: PhantomData,
            _term: PhantomData,
        }
    }
}

impl<TTerm, TStorage> IndexBuilder<TTerm, TStorage>
    where TTerm: Ord + Hash,
          TStorage: Storage<IndexingChunk> + Volatile + 'static
{
    /// Creates a volatile instance of `BooleanIndex`
    /// At the moment `BooleanIndex` does not support adding or removing
    /// documents from the index.
    /// So all index which should be indexed need to be passed to this method
    pub fn create<TCollection, TDoc>(&self, documents: TCollection) -> Result<BooleanIndex<TTerm>>
        where TCollection: Iterator<Item = TDoc>,
              TDoc: Iterator<Item = TTerm>
    {
        BooleanIndex::new(TStorage::new(), documents)
    }
}

impl<TTerm, TStorage> IndexBuilder<TTerm, TStorage>
    where TTerm: Ord + ByteDecodable + ByteEncodable + Hash,
          TStorage: Storage<IndexingChunk> + Persistent + 'static
{
    /// Enables a persistent index at the passed path.
    /// `at` must be either a prefilled directory if the `load` method is to be
    /// called
    /// or an empty directory if a new index is created with
    /// `create_persistence`
    pub fn persist(mut self, at: &Path) -> Self {
        self.persistence = Some(at.to_path_buf());
        self
    }

    /// A new index is created and saved in the directory passed to the
    /// `persist` method
    /// Returns a `BuilderError` if `persist` was not called or if directory is
    /// not empty
    pub fn create_persistent<TDocsIterator, TDocIterator>(&self,
                                                          documents: TDocsIterator)
                                                          -> Result<BooleanIndex<TTerm>>
        where TDocsIterator: Iterator<Item = TDocIterator>,
              TDocIterator: Iterator<Item = TTerm>
    {
        let path = try!(self.check_persist_path(false));
        BooleanIndex::new_persistent(TStorage::create(path).unwrap(), documents, path)
    }

    /// Loads an index from a previously filled directory.
    /// Returns a `BuilderError` if directory is empty or does not contain
    /// valid data
    pub fn load(&self) -> Result<BooleanIndex<TTerm>> {
        let path = try!(self.check_persist_path(true));
        BooleanIndex::load::<TStorage>(path)
    }

    fn check_persist_path(&self, check_for_existing_files: bool) -> Result<&Path> {
        if let Some(ref path) = self.persistence {
            if path.is_dir() {
                let paths = try!(fs::read_dir(path));
                // Path is a directory and seems to exist. Lets see if all the files are present
                if !check_for_existing_files {
                    return Ok(path);
                }
                let mut required_files = Self::required_files();
                for path in paths.filter(|p| p.is_ok()).map(|p| p.unwrap()) {
                    if let Some(pos) = required_files.iter().position(|f| (**f) == path.file_name()) {
                        required_files.swap_remove(pos);
                    }
                }
                if required_files.is_empty() {
                    Ok(path)
                } else {
                    Err(Error::MissingIndexFiles(required_files))
                }
            } else {
                Err(Error::PersistPathIsFile)
            }
        } else {
            Err(Error::PersistPathNotSpecified)
        }
    }

    fn required_files() -> Vec<&'static str> {
        let mut required_files = REQUIRED_FILES.to_vec();
        required_files.extend_from_slice(TStorage::associated_files());
        required_files
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use index::boolean_index::Error;
    use storage::FsStorage;
    use std::env;
    use std::fs;

    #[test]
    fn empty_folder() {
        let path = &env::temp_dir().join("perlin_test_empty_dir");
        fs::create_dir_all(path).unwrap();

        let result = IndexBuilder::<usize, FsStorage<_>>::new().persist(path).load();
        assert!(if let Err(Error::MissingIndexFiles(_)) = result {
            true
        } else {
            false
        });
    }

    #[test]
    fn file_not_folder() {
        let path = &env::temp_dir().join("perlin_test_file.bin");
        fs::File::create(path).unwrap();

        let result = IndexBuilder::<usize, FsStorage<_>>::new().persist(path).load();
        assert!(if let Err(Error::PersistPathIsFile) = result {
            true
        } else {
            false
        });
    }

    #[test]
    fn corrupt_file() {
        let path = &env::temp_dir().join("perlin_corrupted_files_test_dir");
        fs::create_dir_all(path).unwrap();
        for file in IndexBuilder::<usize, FsStorage<_>>::required_files() {
            fs::File::create(path.join(file)).unwrap();
        }

        let result = IndexBuilder::<usize, FsStorage<_>>::new().persist(path).load();
        assert!(if let Err(Error::CorruptedIndexFile(_)) = result {
            true
        } else {
            false
        });
    }
}
