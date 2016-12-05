use std::fs;
use std::marker::PhantomData;
use std::path::{PathBuf, Path};
use std::hash::Hash;

use storage::Storage;
use storage::{ByteEncodable, ByteDecodable};
use chunked_storage::{ChunkedStorage, IndexingChunk};
use utils::persistence::{Volatile, Persistent};


use index::boolean_index;
use index::boolean_index::DocumentTerms;
use index::boolean_index::{Result, Error, BooleanIndex};

const REQUIRED_FILES: [&'static str; 2] = [boolean_index::VOCAB_FILENAME, boolean_index::STATISTICS_FILENAME];

/// `IndexBuilder` is used to build `BooleanIndex` instances
pub struct IndexBuilder<TTerm, TStorage, TDocStorage> {
    persistence: Option<PathBuf>,
    _storage: PhantomData<TStorage>,
    _doc_storage: PhantomData<TDocStorage>,
    _term: PhantomData<TTerm>,
}

impl<TTerm, TStorage, TDocStorage> IndexBuilder<TTerm, TStorage, TDocStorage>
    where TTerm: Ord,
          TStorage: Storage<IndexingChunk>,
          TDocStorage: Storage<DocumentTerms>
{
    /// Creates a new instance of `IndexBuilder`
    pub fn new() -> Self {
        IndexBuilder {
            persistence: None,
            _storage: PhantomData,
            _term: PhantomData,
            _doc_storage: PhantomData
        }
    }
}

impl<TTerm, TStorage, TDocStorage> IndexBuilder<TTerm, TStorage, TDocStorage>
    where TTerm: Ord + Hash,
          TStorage: Storage<IndexingChunk> + Volatile + 'static,
          TDocStorage: Storage<DocumentTerms> + Volatile + 'static

{
    /// Creates a volatile instance of `BooleanIndex`
    /// At the moment `BooleanIndex` does not support adding or removing
    /// documents from the index.
    /// So all index which should be indexed need to be passed to this method
    pub fn create<TCollection, TDoc>(&self, documents: TCollection) -> Result<BooleanIndex<TTerm>>
        where TCollection: Iterator<Item = TDoc>,
              TDoc: Iterator<Item = TTerm>
    {
        BooleanIndex::new(documents, TStorage::new(), TDocStorage::new())
    }
}

impl<TTerm, TStorage, TDocStorage> IndexBuilder<TTerm, TStorage, TDocStorage>
    where TTerm: Ord + ByteDecodable + ByteEncodable + Hash,
          TStorage: Storage<IndexingChunk> + Persistent + 'static,
          TDocStorage: Storage<DocumentTerms> + Persistent + 'static
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
        fs::create_dir_all(&path.join(boolean_index::DOCUMENTS_PATH))?;
        fs::create_dir_all(&path.join(boolean_index::INV_INDEX_PATH))?;
        let index_store = TStorage::create(&path.join(boolean_index::INV_INDEX_PATH))?;
        let doc_store = TDocStorage::create(&path.join(boolean_index::DOCUMENTS_PATH))?;        
        BooleanIndex::new_persistent(documents, index_store, doc_store, path)
    }

    /// Loads an index from a previously filled directory.
    /// Returns a `BuilderError` if directory is empty or does not contain
    /// valid data
    pub fn load(&self) -> Result<BooleanIndex<TTerm>> {
        let path = try!(self.check_persist_path(true));
        BooleanIndex::load::<TStorage, TDocStorage>(path)
    }

    fn check_persist_path(&self, check_for_existing_files: bool) -> Result<&Path> {
        if let Some(ref path) = self.persistence {
            if path.is_dir() {
                let paths = try!(fs::read_dir(path));
                // Path is a directory and seems to exist. Lets see if all the files are present
                if !check_for_existing_files {
                    return Ok(path);
                }
                let mut required_files = REQUIRED_FILES.clone().to_vec();
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
        let required_files = REQUIRED_FILES.to_vec();
        required_files
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use test_utils::{test_dir, create_test_dir};
    use index::boolean_index::Error;
    use storage::FsStorage;
    use std::fs;

    #[test]
    fn empty_folder() {
        let path = &create_test_dir("empty_dir");

        let result = IndexBuilder::<usize, FsStorage<_>, FsStorage<_>>::new().persist(path).load();
        // That is not really beautiful or anything.
        assert!(if let Err(Error::MissingIndexFiles(_)) = result {
            true
        } else {
            false
        });
    }

    #[test]
    fn index_dir_is_file() {
        let path = &test_dir().join("index_dir_is_file.bin");
        fs::File::create(path).unwrap();
        let result = IndexBuilder::<usize, FsStorage<_>, FsStorage<_>>::new().persist(path).load();
        assert!(if let Err(Error::PersistPathIsFile) = result {
            true
        } else {
            false
        });
    }

    #[test]
    fn corrupt_file() {
        let path = &create_test_dir("corrupted_files");
        for file in IndexBuilder::<usize, FsStorage<_>, FsStorage<_>>::required_files() {
            fs::File::create(path.join(file)).unwrap();
        }

        let result = IndexBuilder::<usize, FsStorage<_>, FsStorage<_>>::new().persist(path).load();
        assert!(if let Err(Error::CorruptedIndexFile(_)) = result {
            true
        } else {
            false
        });
    }

    #[test]
    fn required_files_correct() {
        let path = &create_test_dir("required_files_correct");
        fs::create_dir_all(path).unwrap();
        IndexBuilder::<_, FsStorage<_>, FsStorage<_>>::new()
            .persist(path)
            .create_persistent(vec![(0..10).collect::<Vec<_>>().into_iter(),
                                    (0..10)
                                        .map(|i| i * 2)
                                        .collect::<Vec<_>>()
                                        .into_iter(),
                                    vec![5u32, 4, 3, 2, 1, 0].into_iter()]
                .into_iter())
            .unwrap();
        assert_eq!(fs::read_dir(path).unwrap().count(),
                   IndexBuilder::<usize, FsStorage<_>, FsStorage<_>>::required_files().len());
    }
}
