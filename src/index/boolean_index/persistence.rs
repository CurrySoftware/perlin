// Code status in this module: Sketchy.
// TODO: Implement error handling, comment code, write documentation

use std::path::{Path, PathBuf};
use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::{Read, Write};

use utils::byte_code::{ByteEncodable, ByteDecodable};
use utils::persistence::Persistence;
use utils::compression::{vbyte_encode, VByteDecoder};

use index::Index;
use index::storage::Storage;
use index::boolean_index::BooleanIndex;
use index::boolean_index::posting::Listing;

const VOCAB_FILENAME: &'static str = "vocabulary.bin";
const STATISTICS_FILENAME: &'static str = "statistics.bin";
const CHUNKSIZE: usize = 1_000_000;


/// Acts as Wrapper around BooleanIndex provides means of persistence.
/// Use `PersistentBooleanIndex::new` to create a new instance
/// and reload it with `PersistentBooleanIndex::load`
// I am not quite happy with this solution.
// But alas, rust does not offer design by introspection
pub struct PersistentBooleanIndex<TTerm: Ord + ByteEncodable + ByteDecodable>
{
    path: PathBuf,
    index: BooleanIndex<TTerm>,
}

impl<TTerm> PersistentBooleanIndex<TTerm>
    where TTerm: Ord + ByteEncodable + ByteDecodable,          
{
    fn save_vocabulary(&self) {
        // Open file
        let mut vocab_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(self.path.join(VOCAB_FILENAME))
            .unwrap();
        // Iterate over vocabulary and encode data
        let mut byte_buffer = Vec::with_capacity(2 * CHUNKSIZE);
        for vocab_entry in &self.index.term_ids {
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
            .open(self.path.join(STATISTICS_FILENAME))
            .unwrap();
        statistics_file.write(&vbyte_encode(self.index.document_count));
    }

    fn load_statistics(path: &Path) -> usize {
        let mut statistics_file =
            OpenOptions::new().read(true).open(path.join(STATISTICS_FILENAME)).unwrap();
        let mut bytes = Vec::new();
        statistics_file.read_to_end(&mut bytes);
        VByteDecoder::new(bytes.into_iter()).next().unwrap()
    }

    pub fn new<TStorage: Storage<Listing> + Persistence + 'static>(path: &Path) -> Self {
        PersistentBooleanIndex {
            path: path.to_owned(),
            index: BooleanIndex::new(Box::new(TStorage::new(path))),
        }
    }

    pub fn load<TStorage: Storage<Listing> + Persistence + 'static>(path: &Path) -> Self {
        let storage = TStorage::load(path);
        let vocab = Self::load_vocabulary(path);
        let doc_count = Self::load_statistics(path);
        PersistentBooleanIndex {
            path: path.to_owned(),
            index: BooleanIndex::from_parts(Box::new(storage), vocab, doc_count),
        }
    }
}

impl<'a, TTerm> Index<'a, TTerm> for PersistentBooleanIndex<TTerm>
    where TTerm: Ord + ByteEncodable + ByteDecodable
{
    // How utterly ugly
    type Query = <BooleanIndex<TTerm> as Index<'a, TTerm>>::Query;
    type QueryResult = <BooleanIndex<TTerm> as Index<'a, TTerm>>::QueryResult;

    fn index_documents<TDocsIterator: Iterator<Item = Vec<TTerm>>>(&mut self,
                                                             documents: TDocsIterator)
                                                             -> Vec<u64> {
        let result = self.index.index_documents(documents);
        self.save_vocabulary();
        self.save_statistics();
        result
    }

    fn execute_query(&'a self, query: &Self::Query) -> Self::QueryResult {
        self.index.execute_query(query)
    }
}



#[cfg(test)]
mod tests {
    use std::fs::create_dir_all;
    use std::path::Path;

    use super::*;
    use index::Index;
    use index::boolean_index::*;
    use index::storage::fs_storage::FsStorage;

    #[test]
    fn simple() {
        create_dir_all(Path::new("/tmp/persistent_index_test"));
        {
            let mut index: PersistentBooleanIndex<usize> =
                PersistentBooleanIndex::new::<FsStorage<_>>(Path::new("/tmp/persistent_index_test"));
            index.index_documents(vec![(0..10).collect::<Vec<_>>(),
                                       (0..10).map(|i| i * 2).collect::<Vec<_>>(),
                                       vec![5, 4, 3, 2, 1, 0]].into_iter());

            // Test QueryAtoms
            assert!(index.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 7)))
                .collect::<Vec<_>>() == vec![0]);
            assert!(index.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 5)))
                .collect::<Vec<_>>() == vec![0, 2]);
            assert!(index.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 0)))
                .collect::<Vec<_>>() == vec![0, 1, 2]);
            assert!(index.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 16)))
                .collect::<Vec<_>>() == vec![1]);

            // Test NAryQueries
            assert!(index.execute_query(&BooleanQuery::NAry(BooleanOperator::And,
                                                   vec![BooleanQuery::Atom(QueryAtom::new(0, 5)),
                                                    BooleanQuery::Atom(QueryAtom::new(0, 0))]))
                .collect::<Vec<_>>() == vec![0, 2]);
            assert!(index.execute_query(&BooleanQuery::NAry(BooleanOperator::And,
                                                   vec![BooleanQuery::Atom(QueryAtom::new(0, 0)),
                                                    BooleanQuery::Atom(QueryAtom::new(0, 5))]))
                .collect::<Vec<_>>() == vec![0, 2]);
        }
        // Old Index out of scope
        // Load new Index from Folder
        let index2: PersistentBooleanIndex<usize> =
            PersistentBooleanIndex::load::<FsStorage<_>>(Path::new("/tmp/persistent_index_test"));

        // Test QueryAtoms
        assert!(index2.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 7)))
            .collect::<Vec<_>>() == vec![0]);
        assert!(index2.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 5)))
            .collect::<Vec<_>>() == vec![0, 2]);
        assert!(index2.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 0)))
            .collect::<Vec<_>>() == vec![0, 1, 2]);
        assert!(index2.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 16)))
            .collect::<Vec<_>>() == vec![1]);

        // Test NAryQueries
        assert!(index2.execute_query(&BooleanQuery::NAry(BooleanOperator::And,
                                               vec![BooleanQuery::Atom(QueryAtom::new(0, 5)),
                                                    BooleanQuery::Atom(QueryAtom::new(0, 0))]))
            .collect::<Vec<_>>() == vec![0, 2]);
        assert!(index2.execute_query(&BooleanQuery::NAry(BooleanOperator::And,
                                               vec![BooleanQuery::Atom(QueryAtom::new(0, 0)),
                                                    BooleanQuery::Atom(QueryAtom::new(0, 5))]))
            .collect::<Vec<_>>() == vec![0, 2]);
    }


    #[test]
    fn string_index() {
        create_dir_all(Path::new("/tmp/persistent_index_test2"));
        {
            let mut index: PersistentBooleanIndex<String> =
                PersistentBooleanIndex::new::<FsStorage<_>>(Path::new("/tmp/persistent_index_test2"));
            index.index_documents(vec!["a b c d e f g"
                                           .split_whitespace()
                                           .map(|p| p.to_string())
                                           .collect::<Vec<_>>(),
                                       "red blue green yellow pink white black yellow"
                                           .split_whitespace()
                                           .map(|p| p.to_string())
                                           .collect::<Vec<_>>(),
                                       "a c d c"
                                           .split_whitespace()
                                           .map(|p| p.to_string())
                                           .collect::<Vec<_>>(),
                                       "i hate software lets do some carpentry"
                                           .split_whitespace()
                                           .map(|p| p.to_string())
                                           .collect::<Vec<_>>()].into_iter());

            assert!(index.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, "a".to_string())))
                .collect::<Vec<_>>() == vec![0, 2]);
            assert!(index.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, "b".to_string())))
                .collect::<Vec<_>>() == vec![0]);
            assert!(index.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, "black".to_string())))
                    .collect::<Vec<_>>() == vec![1]);
            assert!(index.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, "software".to_string())))
                    .collect::<Vec<_>>() == vec![3]);

        }
        let index2: PersistentBooleanIndex<String> = PersistentBooleanIndex::load::<FsStorage<_>>(Path::new("/tmp/persistent_index_test2"));
            assert!(index2.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, "a".to_string())))
                .collect::<Vec<_>>() == vec![0, 2]);
            assert!(index2.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, "b".to_string())))
                .collect::<Vec<_>>() == vec![0]);
            assert!(index2.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, "black".to_string())))
                    .collect::<Vec<_>>() == vec![1]);
            assert!(index2.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, "software".to_string())))
                    .collect::<Vec<_>>() == vec![3]);
    }
}
