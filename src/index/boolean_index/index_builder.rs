use std::marker::PhantomData;
use std::path::Path;

use utils::byte_code::{ByteEncodable, ByteDecodable};
use utils::persistence::{Volatile, Persistent};

use index::storage::Storage;

use index::boolean_index::{BuilderError, IndexBuilder, BooleanIndex};
use index::boolean_index::posting::Listing;


impl<TTerm, TStorage> IndexBuilder<TTerm, TStorage>
    where TTerm: Ord,
          TStorage: Storage<Listing>
{
    ///Creates a new instance of `IndexBuilder`
    pub fn new() -> Self {
        IndexBuilder {
            persistence: None,
            _storage: PhantomData,
            _term: PhantomData,
        }
    }
}

impl<TTerm, TStorage> IndexBuilder<TTerm, TStorage>
    where TTerm: Ord,
          TStorage: Storage<Listing> + Volatile + 'static
{
    ///Creates a volatile instance of `BooleanIndex`
    ///At the moment `BooleanIndex` does not support adding or removing documents from the index.
    ///So all index which should be indexed need to be passed to this method
    pub fn create<TCollection, TDoc>(&self,
                                     documents: TCollection)
                                     -> Result<BooleanIndex<TTerm>, BuilderError>
        where TCollection: Iterator<Item = TDoc>,
              TDoc: Iterator<Item = TTerm>
    {
        Ok(BooleanIndex::new(TStorage::new(), documents))
    }
}

impl<TTerm, TStorage> IndexBuilder<TTerm, TStorage>
    where TTerm: Ord + ByteDecodable + ByteEncodable,
          TStorage: Storage<Listing> + Persistent + 'static
{
    ///Enables a persistent index at the passed path.
    ///`at` must be either a prefilled directory if the `load` method is to be called
    ///or an empty directory if a new index is created with `create_persistence`    
    pub fn persist(mut self, at: &Path) -> Self {
        self.persistence = Some(at.to_path_buf());
        self
    }

    ///A new index is created and saved in the directory passed to the `persist` method
    ///Returns a `BuilderError` if `persist` was not called or if directory is not empty
    pub fn create_persistent<TDocsIterator, TDocIterator>
        (&self,
         documents: TDocsIterator)
         -> Result<BooleanIndex<TTerm>, BuilderError>
        where TDocsIterator: Iterator<Item = TDocIterator>,
              TDocIterator: Iterator<Item = TTerm>
    {
        if let Some(ref path) = self.persistence {
            Ok(BooleanIndex::new_persistent(TStorage::create(path), documents, path))
        } else {
            Err(BuilderError::PersistPathNotSpecified)
        }
    }

    ///Loads an index from a previously filled directory.
    ///Returns a `BuilderError` if directory is empty or does not contain valid data 
    pub fn load(&self) -> Result<BooleanIndex<TTerm>, BuilderError> {
        if let Some(ref path) = self.persistence {
            Ok(BooleanIndex::load::<TStorage>(path))
        } else {
            Err(BuilderError::PersistPathNotSpecified)
        }
    }
}
