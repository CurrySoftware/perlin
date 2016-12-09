//! This module provides the implementation for boolean information retrieval
//! Use `IndexBuilder` to build indices
//! Use `QueryBuilder` to build queries that run on these indices
use std;
use std::io;
use std::path::{Path, PathBuf};
use std::collections::HashMap;
use std::iter::Iterator;
use std::fs::OpenOptions;
use std::io::{Read, Write};
use std::hash::Hash;
use std::sync::mpsc;

use index::Index;
use storage::{Storage, StorageError};
use storage::DecodeResult;
use chunked_storage::{ChunkedStorage, IndexingChunk};
use index::boolean_index::boolean_query::{BooleanQuery, BooleanOperator, PositionalOperator, FilterOperator, QueryAtom};
use index::boolean_index::indexing::index_documents;
use index::boolean_index::query_result_iterator::*;
use index::boolean_index::query_result_iterator::nary_query_iterator::*;
use index::boolean_index::query_result_iterator::positional_query_iterator::*;
use index::boolean_index::posting::PostingDecoder;

use storage::compression::{EncodingScheme, DecodingScheme, VByteCode};
use storage::{ByteEncodable, ByteDecodable, DecodeError};
use storage::persistence::{Persistent, PersistenceError};

pub use index::boolean_index::query_builder::QueryBuilder;
pub use index::boolean_index::index_builder::IndexBuilder;

mod query_result_iterator;
mod index_builder;
mod query_builder;
mod posting;
mod boolean_query;
mod indexing;

const VOCAB_FILENAME: &'static str = "vocabulary.bin";
const STATISTICS_FILENAME: &'static str = "statistics.bin";
const DOCUMENTS_PATH: &'static str = "docs";
const INV_INDEX_PATH: &'static str = "index";
const CHUNKSIZE: usize = 1_000_000;

/// A specialized `Result` type for operations related to `BooleanIndex`
pub type Result<T> = std::result::Result<T, Error>;

type DocumentTerms = Vec<u8>;

impl ByteEncodable for DocumentTerms {
    fn encode(&self) -> Vec<u8> {
        self.clone()
    }
}

impl ByteDecodable for DocumentTerms {
    fn decode<R: Read>(read: &mut R) -> DecodeResult<Self> {
        let mut result = Vec::new();
        read.read_to_end(&mut result)?;
        Ok(result)
    }
}

#[derive(Debug)]
/// Error kinds that can occure during indexing operations
pub enum IndexingError {
    /// An Error related to sending via `mpsc::Channel`
    Send,
    /// An indexing thread panicked
    ThreadPanic,
}

#[derive(Debug)]
/// Error kinds that can occur during operations related to `BooleanIndex`
pub enum Error {
    /// An Error occured during a persistence operation
    Persistence(PersistenceError),
    /// An IO-Error occured
    IO(io::Error),
    /// A Storage-Error occured
    Storage(StorageError),
    /// An error occured during indexing
    Indexing(IndexingError),
    /// Tried to load a `BooleanIndex` from a corrupted file
    CorruptedIndexFile(Option<DecodeError>),
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Self {
        Error::IO(err)
    }
}

impl From<StorageError> for Error {
    fn from(err: StorageError) -> Self {
        Error::Storage(err)
    }
}

impl From<PersistenceError> for Error {
    fn from(err: PersistenceError) -> Self {
        Error::Persistence(err)
    }
}

impl<T> From<mpsc::SendError<T>> for Error {
    fn from(_: mpsc::SendError<T>) -> Self {
        Error::Indexing(IndexingError::Send)
    }
}

/// Implements the `Index` trait. Limited to boolean retrieval.
pub struct BooleanIndex<TTerm: Ord + Hash> {
    document_count: usize,
    term_ids: HashMap<TTerm, u64>,
    chunked_postings: ChunkedStorage,
    documents: Box<Storage<DocumentTerms>>,
    persist_path: Option<PathBuf>,
}

// Index implementation
impl<'a, TTerm: Ord + Hash> Index<'a, TTerm> for BooleanIndex<TTerm> {
    type Query = BooleanQuery<TTerm>;
    type QueryResult = Box<Iterator<Item = u64> + 'a>;

    /// Executes a `BooleanQuery` and returns a boxed iterator over the resulting document ids.
    /// The query execution is lazy.
    fn execute_query(&'a self, query: &Self::Query) -> Self::QueryResult {
        Box::new(self.run_query(query).map(|p| p.0))
    }
}

impl<TTerm> BooleanIndex<TTerm>
    where TTerm: Ord + ByteDecodable + ByteEncodable + Hash
{
    /// Load a `BooleanIndex` from a previously populated folder
    /// Not intended for public use. Please use the `IndexBuilder` instead
    fn load<TStorage, TDocStorage>(path: &Path) -> Result<Self>
        where TStorage: Storage<IndexingChunk> + Persistent + 'static,
              TDocStorage: Storage<DocumentTerms> + Persistent + 'static
    {
        let storage = try!(TStorage::load(&path.join(INV_INDEX_PATH)));
        let vocab = try!(Self::load_vocabulary(path));
        let doc_count = try!(Self::load_statistics(path));
        let chunked_storage = ChunkedStorage::load(&path.join(INV_INDEX_PATH), Box::new(storage)).unwrap();
        let doc_storage = TDocStorage::load(&path.join(DOCUMENTS_PATH)).unwrap();
        BooleanIndex::from_parts(chunked_storage, vocab, Box::new(doc_storage), doc_count)
    }

    /// Creates a new `BooleanIndex` instance which is written to the passed
    /// path
    /// Not intended for public use. Please use the `IndexBuilder` instead
    fn new_persistent<TDocsIterator, TDocIterator, TStorage, TDocStorage>(documents: TDocsIterator,
                                                                          storage: TStorage,
                                                                          doc_storage: TDocStorage,
                                                                          path: &Path)
                                                                          -> Result<Self>
        where TDocsIterator: Iterator<Item = TDocIterator>,
              TDocIterator: Iterator<Item = TTerm>,
              TStorage: Storage<IndexingChunk> + Persistent + 'static,
              TDocStorage: Storage<DocumentTerms> + Persistent + 'static
    {
        let (document_count, chunked_postings, doc_store, term_ids) = index_documents(documents, storage, doc_storage)?;
        let index = BooleanIndex {
            document_count: document_count,
            term_ids: term_ids,
            persist_path: Some(path.to_path_buf()),
            chunked_postings: chunked_postings,
            documents: Box::new(doc_store),
        };
        try!(index.save_vocabulary());
        try!(index.save_statistics());
        try!(index.chunked_postings.persist(&path.join(INV_INDEX_PATH)));
        Ok(index)
    }

    fn save_vocabulary(&self) -> Result<()> {
        if let Some(filename) = self.persist_path.as_ref().map(|p| p.join(VOCAB_FILENAME)) {
            // Open file
            let mut vocab_file = try!(OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(filename));
            // Iterate over vocabulary and encode data
            let mut byte_buffer = Vec::with_capacity(2 * CHUNKSIZE);
            for vocab_entry in &self.term_ids {
                // Encode term and number of its bytes
                let term_bytes = vocab_entry.0.encode();
                // Encode id
                VByteCode::encode_to_stream(*vocab_entry.1 as usize, &mut byte_buffer)?;
                VByteCode::encode_to_stream(term_bytes.len(), &mut byte_buffer)?;
                // Append term to byte_buffer
                byte_buffer.extend_from_slice(&term_bytes);

                // Write if buffer is full
                if byte_buffer.len() > CHUNKSIZE {
                    try!(vocab_file.write(&byte_buffer));
                    byte_buffer.clear();
                }
            }
            if !byte_buffer.is_empty() {
                // If some rests are in the buffer write them to file
                try!(vocab_file.write(&byte_buffer));
            }
            Ok(())
        } else {
            Err(Error::Persistence(PersistenceError::PersistPathNotSpecified))
        }
    }

    fn load_vocabulary(path: &Path) -> Result<HashMap<TTerm, u64>> {
        // Open file
        let vocab_file = try!(OpenOptions::new().read(true).open(path.join(VOCAB_FILENAME)));
        // Create a decoder from that vector
        let mut decoder = VByteCode::decode_from_stream(vocab_file);
        let mut result = HashMap::new();
        // Get the id
        while let Some(id) = decoder.next() {
            // Get the length of the term in bytes
            if let Some(term_len) = decoder.next() {
                let mut term_bytes = vec![0; term_len];
                try!(decoder.read_exact(&mut term_bytes));
                // Read the bytes and decode them
                match TTerm::decode(&mut term_bytes.as_slice()) {
                    Ok(term) => result.insert(term, id as u64),
                    Err(e) => return Err(Error::CorruptedIndexFile(Some(e))),
                };
            } else {
                return Err(Error::CorruptedIndexFile(None));
            }
        }
        Ok(result)
    }

    fn save_statistics(&self) -> Result<()> {
        // Open file
        if let Some(filename) = self.persist_path.as_ref().map(|p| p.join(STATISTICS_FILENAME)) {
            let mut statistics_file = try!(OpenOptions::new()
                .write(true)
                .create(true)
                .truncate(true)
                .open(filename));
            VByteCode::encode_to_stream(self.document_count, &mut statistics_file)?;
            Ok(())
        } else {
            Err(Error::Persistence(PersistenceError::PersistPathNotSpecified))
        }
    }

    fn load_statistics(path: &Path) -> Result<usize> {
        let statistics_file = try!(OpenOptions::new().read(true).open(path.join(STATISTICS_FILENAME)));
        if let Some(doc_count) = VByteCode::decode_from_stream(statistics_file).next() {
            Ok(doc_count)
        } else {
            Err(Error::CorruptedIndexFile(None))
        }
    }
}

impl<TTerm: Ord + Hash> BooleanIndex<TTerm> {
    /// Returns the number of indexed documents
    pub fn document_count(&self) -> usize {
        self.document_count
    }

    /// Creates a new volatile `BooleanIndex`. Not intended for public use.
    /// Please use `IndexBuilder` instead
    fn new<TDocsIterator, TDocIterator, TStorage, TDocStorage>(documents: TDocsIterator,
                                                               storage: TStorage,
                                                               doc_storage: TDocStorage)
                                                               -> Result<Self>
        where TDocsIterator: Iterator<Item = TDocIterator>,
              TDocIterator: Iterator<Item = TTerm>,
              TStorage: Storage<IndexingChunk> + 'static,
              TDocStorage: Storage<DocumentTerms> + 'static
    {
        let (document_count, chunked_postings, doc_storage, term_ids) =
            try!(index_documents(documents, storage, doc_storage));
        let index = BooleanIndex {
            document_count: document_count,
            term_ids: term_ids,
            persist_path: None,
            chunked_postings: chunked_postings,
            documents: Box::new(doc_storage),
        };
        Ok(index)
    }




    fn from_parts(inverted_index: ChunkedStorage,
                  vocabulary: HashMap<TTerm, u64>,
                  doc_storage: Box<Storage<DocumentTerms>>,
                  document_count: usize)
                  -> Result<Self> {
        Ok(BooleanIndex {
            document_count: document_count,
            term_ids: vocabulary,
            chunked_postings: inverted_index,
            documents: doc_storage,
            persist_path: None,
        })
    }




    fn run_query<'a>(&'a self, query: &BooleanQuery<TTerm>) -> QueryResultIterator<'a> {
        match *query {
            BooleanQuery::Atom(ref atom) => self.run_atom(&atom.query_term),
            BooleanQuery::NAry(ref operator, ref operands) => self.run_nary_query(operator, operands),
            BooleanQuery::Positional(ref operator, ref operands) => self.run_positional_query(operator, operands),
            BooleanQuery::Filter(ref operator, ref sand, ref sieve) => {
                self.run_filter(operator, sand.as_ref(), sieve.as_ref())
            }

        }

    }

    fn run_nary_query<'a>(&'a self,
                          operator: &BooleanOperator,
                          operands: &[BooleanQuery<TTerm>])
                          -> QueryResultIterator<'a> {
        let mut ops = Vec::new();
        for operand in operands {
            ops.push(self.run_query(operand).peekable_seekable())
        }
        QueryResultIterator::NAry(NAryQueryIterator::new(*operator, ops))
    }

    fn run_positional_query(&self,
                            operator: &PositionalOperator,
                            operands: &[QueryAtom<TTerm>])
                            -> QueryResultIterator {
        let mut ops = Vec::with_capacity(operands.len());
        let mut pattern = Vec::with_capacity(operands.len());
        for operand in operands {
            if let Some(id) = self.resolve_term(&operand.query_term) {
                ops.push(QueryResultIterator::Atom(PostingDecoder::new(self.chunked_postings.get(id)))
                    .peekable_seekable());
                pattern.push((operand.relative_position as u32, id));
            } else {
                return QueryResultIterator::Empty;
            }
        }
        QueryResultIterator::Positional(
            PositionalQueryIterator::new(*operator,
                                         QueryResultIterator::NAry(
                                             NAryQueryIterator::new(
                                                 BooleanOperator::And, ops)
                                         )
                                         .peekable_seekable(),
                                         pattern,
                                         self.documents.as_ref()))

    }

    fn run_filter<'a>(&'a self,
                      operator: &FilterOperator,
                      sand: &BooleanQuery<TTerm>,
                      sieve: &BooleanQuery<TTerm>)
                      -> QueryResultIterator<'a> {
        QueryResultIterator::Filter(FilterIterator::new(*operator,
                                                        Box::new(self.run_query(sand).peekable_seekable()),
                                                        Box::new(self.run_query(sieve).peekable_seekable())))
    }


    fn run_atom(&self, atom: &TTerm) -> QueryResultIterator {
        if let Some(id) = self.resolve_term(atom) {
            QueryResultIterator::Atom(PostingDecoder::new(self.chunked_postings.get(id)))
        } else {
            QueryResultIterator::Empty
        }
    }

    fn resolve_term(&self, atom: &TTerm) -> Option<u64> {
        self.term_ids.get(atom).map(|id| *id)
    }
}



// --- Tests

#[cfg(test)]
mod tests {
    use super::*;

    use test_utils::create_test_dir;

    use storage::{FsStorage, RamStorage};
    use index::boolean_index::boolean_query::*;
    use index::Index;



    pub fn prepare_index() -> BooleanIndex<usize> {
        let index = IndexBuilder::<_, RamStorage<_>, RamStorage<_>>::new()
            .create(vec![(0..10).collect::<Vec<_>>().into_iter(),
                         (0..10)
                             .map(|i| i * 2)
                             .collect::<Vec<_>>()
                             .into_iter(),
                         vec![5, 4, 3, 2, 1, 0].into_iter()]
                .into_iter());
        index.unwrap()
    }

    #[test]
    fn empty_query() {
        let index = prepare_index();
        assert!(index.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 15)))
            .collect::<Vec<_>>() == vec![]);

    }



    #[test]
    fn indexing() {
        let index = prepare_index();
        // Check number of docs
        assert!(index.document_count == 3);
        // Check number of terms (0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 14, 16, 18)
        assert!(index.term_ids.len() == 15);
        // assert!(*index.postings.get(*index.term_ids.get(&0).unwrap()).unwrap() ==
        //         vec![(0, vec![0]), (1, vec![0]), (2, vec![5])]);
        assert_eq!(index.document_count(), 3);

    }

    #[test]
    fn query_atom() {
        let index = prepare_index();

        assert_eq!(index.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 7)))
                       .collect::<Vec<_>>(),
                   vec![0]);
        assert_eq!(index.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 5)))
                       .collect::<Vec<_>>(),
                   vec![0, 2]);
        assert_eq!(index.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 0)))
                       .collect::<Vec<_>>(),
                   vec![0, 1, 2]);
        assert_eq!(index.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 16)))
                       .collect::<Vec<_>>(),
                   vec![1]);
    }

    #[test]
    fn nary_query() {
        let index = prepare_index();

        assert!(index.execute_query(&BooleanQuery::NAry(BooleanOperator::And,
                                               vec![BooleanQuery::Atom(QueryAtom::new(0, 5)),
                                                    BooleanQuery::Atom(QueryAtom::new(0, 0))]))
            .collect::<Vec<_>>() == vec![0, 2]);
        assert!(index.execute_query(&BooleanQuery::NAry(BooleanOperator::And,
                                               vec![BooleanQuery::Atom(QueryAtom::new(0, 0)),
                                                    BooleanQuery::Atom(QueryAtom::new(0, 5))]))
            .collect::<Vec<_>>() == vec![0, 2]);
    }

    #[test]
    fn and_query() {
        let index = prepare_index();
        // assert_eq!(index.execute_query(&BooleanQuery::NAry(BooleanOperator::And,
        //                                        vec![BooleanQuery::Atom(QueryAtom::new(0, 3)),
        //                                             BooleanQuery::Atom(QueryAtom::new(0, 12))]))
        //     .collect::<Vec<_>>(), vec![]);
        assert_eq!(index.execute_query(&BooleanQuery::NAry(BooleanOperator::And,
                                                          vec![BooleanQuery::Atom(QueryAtom::new(0, 14)),
                                                               BooleanQuery::Atom(QueryAtom::new(0, 12))]))
                       .collect::<Vec<_>>(),
                   vec![1]);
        assert_eq!(index.execute_query(&BooleanQuery::NAry(BooleanOperator::And,
                                                          vec![BooleanQuery::NAry(BooleanOperator::And,
                                                                       vec![BooleanQuery::Atom(QueryAtom::new(0, 3)),
                        BooleanQuery::Atom(QueryAtom::new(0, 9))]),
                                                    BooleanQuery::Atom(QueryAtom::new(0, 12))]))
                       .collect::<Vec<_>>(),
                   vec![]);
        assert_eq!(index.execute_query(&BooleanQuery::NAry(BooleanOperator::And,
                                                          vec![BooleanQuery::NAry(BooleanOperator::And,
                                                                       vec![BooleanQuery::Atom(QueryAtom::new(0, 2)),
                        BooleanQuery::Atom(QueryAtom::new(0, 4))]),
                                                    BooleanQuery::Atom(QueryAtom::new(0, 16))]))
                       .collect::<Vec<_>>(),
                   vec![1]);
    }

    #[test]
    fn or_query() {
        let index = prepare_index();
        assert_eq!(index.execute_query(&BooleanQuery::NAry(BooleanOperator::Or,
                                                          vec![BooleanQuery::Atom(QueryAtom::new(0, 3)),
                                                               BooleanQuery::Atom(QueryAtom::new(0, 12))]))
                       .collect::<Vec<_>>(),
                   vec![0, 1, 2]);
        assert_eq!(index.execute_query(&BooleanQuery::NAry(BooleanOperator::Or,
                                                          vec![BooleanQuery::Atom(QueryAtom::new(0, 14)),
                                                               BooleanQuery::Atom(QueryAtom::new(0, 12))]))
                       .collect::<Vec<_>>(),
                   vec![1]);
        assert_eq!(index.execute_query(&BooleanQuery::NAry(BooleanOperator::Or,
                                                          vec![BooleanQuery::NAry(BooleanOperator::Or,
                    vec![BooleanQuery::Atom(QueryAtom::new(0, 3)),
                        BooleanQuery::Atom(QueryAtom::new(0, 9))]
                    ),
                    BooleanQuery::Atom(QueryAtom::new(0, 16))]))
                       .collect::<Vec<_>>(),
                   vec![0, 1, 2]);
    }

    #[test]
    fn inorder_query() {
        let index = prepare_index();
        assert!(index.execute_query(&BooleanQuery::Positional(PositionalOperator::InOrder,
                                                     vec![QueryAtom::new(0, 0), QueryAtom::new(1, 1)]))
            .collect::<Vec<_>>() == vec![0]);
        assert!(index.execute_query(&BooleanQuery::Positional(PositionalOperator::InOrder,
                                                     vec![QueryAtom::new(1, 0), QueryAtom::new(0, 1)]))
            .collect::<Vec<_>>() == vec![2]);
        assert!(index.execute_query(&BooleanQuery::Positional(PositionalOperator::InOrder,
                                                     vec![QueryAtom::new(0, 0), QueryAtom::new(1, 2)]))
            .collect::<Vec<_>>() == vec![1]);
        assert!(index.execute_query(&BooleanQuery::Positional(PositionalOperator::InOrder,
                                                     vec![QueryAtom::new(2, 2),
                                                          QueryAtom::new(1, 1),
                                                          QueryAtom::new(0, 0)]))
            .collect::<Vec<_>>() == vec![0]);
        assert!(index.execute_query(&BooleanQuery::Positional(PositionalOperator::InOrder,
                                                     vec![QueryAtom::new(0, 2),
                                                          QueryAtom::new(1, 1),
                                                          QueryAtom::new(2, 0)]))
            .collect::<Vec<_>>() == vec![2]);
        assert!(index.execute_query(&BooleanQuery::Positional(PositionalOperator::InOrder,
                                                     vec![QueryAtom::new(0, 2),
                                                          QueryAtom::new(1, 1),
                                                          QueryAtom::new(3, 0)]))
            .collect::<Vec<_>>() == vec![]);
    }

    #[test]
    fn query_filter() {
        let index = prepare_index();
        assert!(index.execute_query(
            &BooleanQuery::Filter(FilterOperator::Not,
            Box::new(BooleanQuery::NAry(
                BooleanOperator::And,
                vec![BooleanQuery::Atom(QueryAtom::new(0, 2)),
                     BooleanQuery::Atom(QueryAtom::new(0, 0))])),
                      Box::new(BooleanQuery::Atom(
                          QueryAtom::new(0, 16))))).collect::<Vec<_>>() == vec![0,2]);
    }


    #[test]
    fn persistence() {
        let path = &create_test_dir("persistent_index");
        {
            let index = IndexBuilder::<u32, FsStorage<_>, FsStorage<_>>::new()
                .persist(path)
                .create_persistent(vec![(0..10).collect::<Vec<_>>().into_iter(),
                                        (0..10).map(|i| i * 2).collect::<Vec<_>>().into_iter(),
                                        vec![5, 4, 3, 2, 1, 0].into_iter()]
                    .into_iter())
                .unwrap();

            assert!(index.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 7)))
                .collect::<Vec<_>>() == vec![0]);
            assert!(index.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 5)))
                .collect::<Vec<_>>() == vec![0, 2]);
            assert!(index.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 0)))
                .collect::<Vec<_>>() == vec![0, 1, 2]);
            assert!(index.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 16)))
                .collect::<Vec<_>>() == vec![1]);
        }

        {
            let index = IndexBuilder::<usize, FsStorage<_>, FsStorage<_>>::new()
                .persist(path)
                .load()
                .unwrap();
            assert!(index.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 7)))
                .collect::<Vec<_>>() == vec![0]);
            assert!(index.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 5)))
                .collect::<Vec<_>>() == vec![0, 2]);
            assert!(index.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 0)))
                .collect::<Vec<_>>() == vec![0, 1, 2]);
            assert!(index.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 16)))
                .collect::<Vec<_>>() == vec![1]);
        }
    }
}
