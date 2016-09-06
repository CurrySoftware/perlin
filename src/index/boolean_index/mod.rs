use index::Index;

use std::path::{Path, PathBuf};
use std::collections::BTreeMap;
use std::iter::Iterator;
use std::fs::OpenOptions;
use std::marker::PhantomData;
use std::io::{Error, Read, Write};

use index::storage::Storage;
use index::boolean_index::query_result_iterator::*;
use index::boolean_index::query_result_iterator::nary_query_iterator::*;
use index::boolean_index::posting::{Listing, Posting};

use utils::compression::{vbyte_encode, VByteDecoder};
use utils::owning_iterator::{OwningIterator, ArcIter};
use utils::byte_code::{ByteEncodable, ByteDecodable};
use utils::persistence::Persistent;

mod query_result_iterator;
mod transfer;
mod index_builder;

const VOCAB_FILENAME: &'static str = "vocabulary.bin";
const STATISTICS_FILENAME: &'static str = "statistics.bin";
const CHUNKSIZE: usize = 1_000_000;

type DocumentIterator<TTerm> = Iterator<Item = TTerm>;
type CollectionIterator<TTerm> = Iterator<Item = DocumentIterator<TTerm>>;

#[derive(Debug)]
pub enum BuilderError {
    PersistPathNotSpecified,
    EmptyPersistPath,
    IOError(Error)
}

/// Builds a `BooleanIndex`
pub struct IndexBuilder<TTerm, TStorage> {
    persistence: Option<PathBuf>,
    _storage: PhantomData<TStorage>,
    _term: PhantomData<TTerm>,
}


// not intended for public use. Thus this wrapper module
mod posting {
    use utils::byte_code::{ByteDecodable, ByteEncodable};
    use utils::compression::*;

    // For each term-document pair the doc_id and the
    // positions of the term inside the document are stored
    pub type Posting = (u64 /* doc_id */, Vec<u32> /* positions */);
    pub type Listing = Vec<Posting>;

    impl ByteEncodable for Listing {
        fn encode(&self) -> Vec<u8> {
            let mut bytes: Vec<u8> = Vec::new();
            bytes.append(&mut vbyte_encode(self.len()));
            for posting in self {
                bytes.append(&mut vbyte_encode(posting.0 as usize));
                bytes.append(&mut vbyte_encode(posting.1.len() as usize));
                let mut last_position = 0;
                for position in &posting.1 {
                    bytes.append(&mut vbyte_encode((*position - last_position) as usize));
                    last_position = *position;
                }
            }
            bytes
        }
    }

    // TODO: Errorhandling
    impl ByteDecodable for Vec<Posting> {
        fn decode<TIterator: Iterator<Item = u8>>(bytes: TIterator) -> Result<Self, String> {
            let mut decoder = VByteDecoder::new(bytes);
            let postings_len = decoder.next().unwrap();
            let mut postings = Vec::with_capacity(postings_len);
            for _ in 0..postings_len {
                let doc_id = decoder.next().unwrap();
                let positions_len = decoder.next().unwrap();
                let mut positions = Vec::with_capacity(positions_len as usize);
                let mut last_position = 0;
                for _ in 0..positions_len {
                    last_position += decoder.next().unwrap();
                    positions.push(last_position as u32);
                }
                postings.push((doc_id as u64, positions));
            }
            Ok(postings)
        }
    }
}

/// Basic boolean operator. Use it in combination with a `BooleanQuery`
#[derive(Copy ,Clone)]
pub enum BooleanOperator {
    Or,
    And,
}

/// Basic filter operator. Use it in combination with a `BooleanQuery`
#[derive(Copy, Clone)]
pub enum FilterOperator {
    Not,
}

/// Basic positional operator. Use it in combination with a `BooleanQuery`
#[derive(Copy, Clone)]
pub enum PositionalOperator {
    /// Ensures that QueryAtoms are in the specified order and placement
    /// See `BooleanQuery::Positional` for more information
    InOrder,
}

/// Stores term to be compared against and relative position of a query atom
pub struct QueryAtom<TTerm> {
    relative_position: usize,
    query_term: TTerm,
}

impl<TTerm> QueryAtom<TTerm> {
    pub fn new(relative_position: usize, query_term: TTerm) -> Self {
        QueryAtom {
            relative_position: relative_position,
            query_term: query_term,
        }
    }
}


pub enum BooleanQuery<TTerm> {
    Atom(QueryAtom<TTerm>),

    // Different from NAry because positional queries can currently only run on query-atoms.
    // To ensure correct usage, this rather inelegant abstraction was implemented
    // Nevertheless, internally both are handled by the same code
    // See `NAryQueryIterator::new` and `NAryQueryIterator::new_positional`
    Positional(PositionalOperator, Vec<QueryAtom<TTerm>>),
    NAry(BooleanOperator, Vec<BooleanQuery<TTerm>>),
    Filter(FilterOperator,
           // sand
           Box<BooleanQuery<TTerm>>,
           // sieve
           Box<BooleanQuery<TTerm>>),
}

/// Utility function that builds a positional query from a term iterator
/// Assumes that the terms in the stream are in order
pub fn build_positional_query<TIterator, TTerm>(operator: PositionalOperator,
                                                terms: TIterator)
                                                -> BooleanQuery<TTerm>
    where TIterator: Iterator<Item = TTerm>
{
    BooleanQuery::Positional(operator,
                             terms.enumerate()
                                 .map(|(i, t)| QueryAtom::new(i, t))
                                 .collect::<Vec<_>>())
}

/// Builds an NAry-Query from a term-iterator and a `BooleanOperator`
pub fn build_nary_query<TIterator: Iterator<Item = TTerm>, TTerm>(operator: BooleanOperator,
                                                                  terms: TIterator)
                                                                  -> BooleanQuery<TTerm> {
    BooleanQuery::NAry(operator,
                       terms.map(|t| BooleanQuery::Atom(QueryAtom::new(0, t)))
                           .collect::<Vec<_>>())
}

pub struct BooleanIndex<TTerm: Ord> {
    document_count: usize,
    term_ids: BTreeMap<TTerm, u64>,
    postings: Box<Storage<Listing>>,
    persist_path: Option<PathBuf>,
}

// Index implementation
impl<'a, TTerm: Ord> Index<'a, TTerm> for BooleanIndex<TTerm> {
    type Query = BooleanQuery<TTerm>;
    type QueryResult = Box<Iterator<Item = u64> + 'a>;

    /// Executes a `BooleanQuery` and returns a boxed iterator over the results
    /// The query execution is eager and returns the ids of the documents
    /// TODO: Can we find a lazy solution for that?
    fn execute_query(&'a self, query: &Self::Query) -> Self::QueryResult {
        match self.run_query(query) {
            QueryResultIterator::Empty => Box::new(Vec::<u64>::new().into_iter()),
            QueryResultIterator::Atom(_, iter) => {
                let mut res = Vec::with_capacity(iter.len());
                for _ in 0..iter.len() {
                    res.push(iter.next().unwrap().0)
                }
                Box::new(res.into_iter())
            }
            QueryResultIterator::NAry(iter) => {
                let mut res = Vec::new();
                while let Some(posting) = iter.next() {
                    res.push(posting.0)
                }
                Box::new(res.into_iter())
            }
            QueryResultIterator::Filter(iter) => {
                let mut res = Vec::new();
                while let Some(posting) = iter.next() {
                    res.push(posting.0)
                }
                Box::new(res.into_iter())
            }
        }
    }
}

impl<TTerm> BooleanIndex<TTerm>
    where TTerm: Ord + ByteDecodable + ByteEncodable
{
    /// Load a `BooleanIndex` from a previously populated folder
    /// Not intended for public use. Please use the `IndexBuilder` instead
    fn load<TStorage>(path: &Path) -> Self
        where TStorage: Storage<Listing> + Persistent + 'static
    {
        let storage = TStorage::load(path);
        let vocab = Self::load_vocabulary(path);
        let doc_count = Self::load_statistics(path);
        BooleanIndex::from_parts(Box::new(storage), vocab, doc_count)
    }

    /// Creates a new `BooleanIndex` instance which is written to the passed path
    /// Not intended for public use. Please use the `IndexBuilder` instead
    fn new_persistent<TDocsIterator, TDocIterator, TStorage>(storage: TStorage,
                                                             documents: TDocsIterator,
                                                             path: &Path)
                                                             -> Self
        where TDocsIterator: Iterator<Item = TDocIterator>,
              TDocIterator: Iterator<Item = TTerm>,
              TStorage: Storage<Listing> + Persistent + 'static
    {
        let mut index = BooleanIndex {
            document_count: 0,
            term_ids: BTreeMap::new(),
            postings: Box::new(storage),
            persist_path: Some(path.to_path_buf()),
        };
        index.index_documents(documents);
        index.save_vocabulary();
        index.save_statistics();
        index
    }

    fn save_vocabulary(&self) {
        // Open file
        let mut vocab_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(self.persist_path.as_ref().unwrap().join(VOCAB_FILENAME))
            .unwrap();
        // Iterate over vocabulary and encode data
        let mut byte_buffer = Vec::with_capacity(2 * CHUNKSIZE);
        for vocab_entry in &self.term_ids {
            // Encode term and number of its bytes
            let term_bytes = vocab_entry.0.encode();
            let term_length_bytes = vbyte_encode(term_bytes.len());
            // Encode id
            let id_bytes = vbyte_encode(*vocab_entry.1 as usize);

            // Append id, term length and term to byte_buffer
            byte_buffer.extend_from_slice(&id_bytes);
            byte_buffer.extend_from_slice(&term_length_bytes);
            byte_buffer.extend_from_slice(&term_bytes);

            // Write if buffer is full
            if byte_buffer.len() > CHUNKSIZE {
                vocab_file.write(&byte_buffer);
                byte_buffer.clear();
            }
        }
        vocab_file.write(&byte_buffer);
    }

    fn load_vocabulary(path: &Path) -> BTreeMap<TTerm, u64> {
        // Open file
        let mut vocab_file = OpenOptions::new().read(true).open(path.join(VOCAB_FILENAME)).unwrap();
        let mut bytes = Vec::new();
        vocab_file.read_to_end(&mut bytes);
        let mut decoder = VByteDecoder::new(bytes.into_iter());
        let mut result = BTreeMap::new();
        while let Some(id) = decoder.next() {
            let term_len = decoder.next().unwrap();
            let term = TTerm::decode(decoder.underlying_iterator().take(term_len)).unwrap();
            result.insert(term, id as u64);
        }
        result
    }

    fn save_statistics(&self) {
        // Open file
        let mut statistics_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(self.persist_path.as_ref().unwrap().join(STATISTICS_FILENAME))
            .unwrap();
        statistics_file.write(&vbyte_encode(self.document_count));
    }

    fn load_statistics(path: &Path) -> usize {
        let mut statistics_file =
            OpenOptions::new().read(true).open(path.join(STATISTICS_FILENAME)).unwrap();
        let mut bytes = Vec::new();
        statistics_file.read_to_end(&mut bytes);
        VByteDecoder::new(bytes.into_iter()).next().unwrap()
    }
}

impl<TTerm: Ord> BooleanIndex<TTerm> {
    /// Creates a new volatile `BooleanIndex`. Not intended for public use.
    /// Please use `IndexBuilder` instead
    fn new<TDocsIterator, TDocIterator, TStorage>(storage: TStorage,
                                                  documents: TDocsIterator)
                                                  -> Self
        where TDocsIterator: Iterator<Item = TDocIterator>,
              TDocIterator: Iterator<Item = TTerm>,
              TStorage: Storage<Listing> + 'static
    {
        let mut index = BooleanIndex {
            document_count: 0,
            term_ids: BTreeMap::new(),
            postings: Box::new(storage),
            persist_path: None,
        };
        index.index_documents(documents);
        index
    }


    fn from_parts(inverted_index: Box<Storage<Listing>>,
                  vocabulary: BTreeMap<TTerm, u64>,
                  document_count: usize)
                  -> Self {
        BooleanIndex {
            document_count: document_count,
            term_ids: vocabulary,
            postings: inverted_index,
            persist_path: None,
        }
    }

    /// Indexes a document collection for later retrieval
    /// Returns the document_ids used by the index
    // First Shot. TODO: Needs improvement!
    fn index_documents<TDocsIterator, TDocIterator>(&mut self, documents: TDocsIterator) -> Vec<u64>
        where TDocsIterator: Iterator<Item = TDocIterator>,
              TDocIterator: Iterator<Item = TTerm>
    {
        let mut inv_index: BTreeMap<u64, Vec<Posting>> = BTreeMap::new();
        let mut result = Vec::with_capacity(10);
        // For every document in the collection
        for document in documents {
            // Determine its id. consecutively numbered
            let new_doc_id = self.document_count as u64;
            // Enumerate over its terms
            for (term_position, term) in document.into_iter().enumerate() {
                // Has term already been seen? Is it already in the vocabulary?
                if let Some(term_id) = self.term_ids.get(&term) {
                    // Get its listing from the temporary. And add doc_id and/or position to it
                    let listing = inv_index.get_mut(term_id).unwrap();
                    match listing.binary_search_by(|&(doc_id, _)| doc_id.cmp(&new_doc_id)) {
                        Ok(term_doc_index) => {
                            // Document already had that term.
                            // Look for where to put the current term in the positions list
                            let term_doc_positions =
                                &mut listing.get_mut(term_doc_index).unwrap().1;
                            if let Err(index) =
                                   term_doc_positions.binary_search(&(term_position as u32)) {
                                term_doc_positions.insert(index, term_position as u32)
                            }
                            // Two terms at the same position. Should at least be possible
                            // so do nothing if term_position already exists
                        }
                        Err(term_doc_index) => {
                            listing.insert(term_doc_index,
                                           (new_doc_id as u64, vec![term_position as u32]))
                        }
                    }
                    // Term is indexed. Continue with the next one
                    continue;
                };
                // Term was not yet indexed. Add it
                let term_id = self.term_ids.len() as u64;
                self.term_ids.insert(term, term_id);
                inv_index.insert(term_id, vec![(new_doc_id, vec![term_position as u32])]);
            }
            self.document_count += 1;
            result.push(new_doc_id);
        }

        // everything is now indexed. Hand it to our storage.
        // We do not care where it saves our data.
        for (term_id, listing) in inv_index {
            self.postings.store(term_id, listing).unwrap();
        }

        result
    }


    fn run_query(&self, query: &BooleanQuery<TTerm>) -> QueryResultIterator {
        match *query {
            BooleanQuery::Atom(ref atom) => self.run_atom(atom.relative_position, &atom.query_term),
            BooleanQuery::NAry(ref operator, ref operands) => {
                self.run_nary_query(operator, operands)
            }
            BooleanQuery::Positional(ref operator, ref operands) => {
                self.run_positional_query(operator, operands)
            }
            BooleanQuery::Filter(ref operator, ref sand, ref sieve) => {
                self.run_filter(operator, sand.as_ref(), sieve.as_ref())
            }

        }

    }

    fn run_nary_query(&self,
                      operator: &BooleanOperator,
                      operands: &[BooleanQuery<TTerm>])
                      -> QueryResultIterator {
        let mut ops = Vec::new();
        for operand in operands {
            ops.push(self.run_query(operand))
        }
        QueryResultIterator::NAry(NAryQueryIterator::new(*operator, ops))
    }

    fn run_positional_query(&self,
                            operator: &PositionalOperator,
                            operands: &[QueryAtom<TTerm>])
                            -> QueryResultIterator {
        let mut ops = Vec::new();
        for operand in operands {
            ops.push(self.run_atom(operand.relative_position, &operand.query_term));
        }
        QueryResultIterator::NAry(NAryQueryIterator::new_positional(*operator, ops))
    }

    fn run_filter(&self,
                  operator: &FilterOperator,
                  sand: &BooleanQuery<TTerm>,
                  sieve: &BooleanQuery<TTerm>)
                  -> QueryResultIterator {
        QueryResultIterator::Filter(FilterIterator::new(*operator,
                                                        Box::new(self.run_query(sand)),
                                                        Box::new(self.run_query(sieve))))
    }


    fn run_atom(&self, relative_position: usize, atom: &TTerm) -> QueryResultIterator {
        if let Some(result) = self.term_ids.get(atom) {
            QueryResultIterator::Atom(relative_position,
                                      ArcIter::new(self.postings.get(*result).unwrap()))
        } else {
            QueryResultIterator::Empty
        }
    }
}



// --- Tests

#[cfg(test)]
mod tests {

    use std::fs::create_dir_all;
    use std::path::Path;

    use super::*;
    use index::Index;
    use index::storage::ram_storage::RamStorage;
    use index::storage::fs_storage::FsStorage;


    pub fn prepare_index() -> BooleanIndex<usize> {
        let index = IndexBuilder::<_, RamStorage<_>>::new()
            .create(vec![(0..10).collect::<Vec<_>>().into_iter(),
                         (0..10).map(|i| i * 2).collect::<Vec<_>>().into_iter(),
                         vec![5, 4, 3, 2, 1, 0].into_iter()]
                .into_iter());
        index.unwrap()
    }

    #[test]
    fn positional_query_builder() {
        let index = prepare_index();
        assert_eq!(index.execute_query(&build_positional_query(PositionalOperator::InOrder,
                                                              vec![0, 2, 4, 6, 8].into_iter()))
                       .collect::<Vec<_>>(),
                   vec![1]);
        assert_eq!(index.execute_query(&build_positional_query(PositionalOperator::InOrder,
                                                              vec![0, 1, 2, 4, 6, 8].into_iter()))
                       .collect::<Vec<_>>(),
                   vec![]);
    }

    #[test]
    fn nary_query_builder() {
        let index = prepare_index();
        assert_eq!(index.execute_query(&build_nary_query(BooleanOperator::And,
                                                        vec![6, 4, 3].into_iter()))
                       .collect::<Vec<_>>(),
                   vec![0]);
        assert_eq!(index.execute_query(&build_nary_query(BooleanOperator::Or,
                                                        vec![16, 9].into_iter()))
                       .collect::<Vec<_>>(),
                   vec![0, 1]);
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
        assert!(*index.postings.get(*index.term_ids.get(&0).unwrap()).unwrap() ==
                vec![(0, vec![0]), (1, vec![0]), (2, vec![5])]);
    }

    #[test]
    fn query_atom() {
        let index = prepare_index();

        assert!(index.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 7)))
            .collect::<Vec<_>>() == vec![0]);
        assert!(index.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 5)))
            .collect::<Vec<_>>() == vec![0, 2]);
        assert!(index.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 0)))
            .collect::<Vec<_>>() == vec![0, 1, 2]);
        assert!(index.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 16)))
            .collect::<Vec<_>>() == vec![1]);
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
        assert!(index.execute_query(&BooleanQuery::NAry(BooleanOperator::And,
                                               vec![BooleanQuery::Atom(QueryAtom::new(0, 3)),
                                                    BooleanQuery::Atom(QueryAtom::new(0,
                                                                                      12))]))
            .collect::<Vec<_>>() == vec![]);
        assert!(index.execute_query(&BooleanQuery::NAry(BooleanOperator::And,
                                               vec![BooleanQuery::Atom(QueryAtom::new(0,
                                                                                      14)),
                                                    BooleanQuery::Atom(QueryAtom::new(0,
                                                                                      12))]))
            .collect::<Vec<_>>() == vec![1]);
        assert!(index.execute_query(&BooleanQuery::NAry(BooleanOperator::And,
                                               vec![BooleanQuery::NAry(BooleanOperator::And,
                    vec![BooleanQuery::Atom(QueryAtom::new(0, 3)),
                        BooleanQuery::Atom(QueryAtom::new(0, 9))]
                    ),
                    BooleanQuery::Atom(QueryAtom::new(0, 12))]))
            .collect::<Vec<_>>() == vec![]);
        assert!(index.execute_query(&BooleanQuery::NAry(BooleanOperator::And,
                                               vec![BooleanQuery::NAry(BooleanOperator::And,
                    vec![BooleanQuery::Atom(QueryAtom::new(0, 2)),
                        BooleanQuery::Atom(QueryAtom::new(0, 4))]
                    ),
                    BooleanQuery::Atom(QueryAtom::new(0, 16))]))
            .collect::<Vec<_>>() == vec![1]);
    }

    #[test]
    fn or_query() {
        let index = prepare_index();
        assert_eq!(index.execute_query(&BooleanQuery::NAry(BooleanOperator::Or,
                                               vec![BooleanQuery::Atom(QueryAtom::new(0, 3)),
                                                    BooleanQuery::Atom(QueryAtom::new(0,
                                                                                      12))]))
            .collect::<Vec<_>>(), vec![0, 1, 2]);
        assert_eq!(index.execute_query(&BooleanQuery::NAry(BooleanOperator::Or,
                                                          vec![BooleanQuery::Atom(QueryAtom::new(0,
                                                                                      14)),
                                                    BooleanQuery::Atom(QueryAtom::new(0,
                                                                                      12))]))
                       .collect::<Vec<_>>(),
                   vec![1]);
        assert_eq!(index.execute_query(&BooleanQuery::NAry(BooleanOperator::Or,
                                               vec![BooleanQuery::NAry(BooleanOperator::Or,
                    vec![BooleanQuery::Atom(QueryAtom::new(0, 3)),
                        BooleanQuery::Atom(QueryAtom::new(0, 9))]
                    ),
                    BooleanQuery::Atom(QueryAtom::new(0, 16))]))
            .collect::<Vec<_>>(), vec![0, 1, 2]);
    }

    #[test]
    fn inorder_query() {
        let index = prepare_index();
        assert!(index.execute_query(&BooleanQuery::Positional(PositionalOperator::InOrder,
                                                     vec![QueryAtom::new(0, 0),
                                                          QueryAtom::new(1, 1)]))
            .collect::<Vec<_>>() == vec![0]);
        assert!(index.execute_query(&BooleanQuery::Positional(PositionalOperator::InOrder,
                                                     vec![QueryAtom::new(1, 0),
                                                          QueryAtom::new(0, 1)]))
            .collect::<Vec<_>>() == vec![2]);
        assert!(index.execute_query(&BooleanQuery::Positional(PositionalOperator::InOrder,
                                                     vec![QueryAtom::new(0, 0),
                                                          QueryAtom::new(1, 2)]))
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
        assert!(create_dir_all(Path::new("/tmp/persistent_index_test")).is_ok());
        {
            let index = IndexBuilder::<_, FsStorage<_>>::new()
                .persist(Path::new("/tmp/persistent_index_test"))
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
            let index = IndexBuilder::<usize, FsStorage<_>>::new()
                .persist(Path::new("/tmp/persistent_index_test"))
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
