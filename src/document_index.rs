use std::path::{PathBuf, Path};
use std::marker::PhantomData;

use perlin_core::index::posting::{DocId};

pub type Pipeline<Out, T> = Box<Fn() -> Box<Fn(DocId, &mut T, &str) -> PhantomData<Out>>>;

/// `DocumentIndex` takes some of the basic building blocks in `perlin_core`
/// and provides an abstraction that can be used to index and query documents
/// using fields, metadata, taxonomies etc
pub struct DocumentIndex {
    // We need to overwrite perlin_core's default DocIds as some Documents might contain
    // other fields than others. This counter acts as DocumentIndex global document counter.
    doc_id_counter: DocId,
    // The base path of this index.
    base_path: PathBuf
}

impl DocumentIndex{
    /// Create a new index.
    pub fn new(path: &Path) -> Self {
        DocumentIndex {
            doc_id_counter: DocId::none(),
            base_path: path.to_path_buf(),
        }
    }
    
    pub fn get_next_doc_id(&mut self) -> DocId {
        self.doc_id_counter.inc();
        self.doc_id_counter
    }

    /// Commits this index
    pub fn commit(&mut self) {
    }

}

#[cfg(test)]
mod tests {
    use document::PerlinDocument;
    use document_index::Pipeline;
    use perlin_core::index::posting::DocId;
    use field::Field;

    use rust_stemmers::Algorithm;

    use test_utils::create_test_dir;

    #[derive(PerlinDocument)]
    struct Test {
        text: Field<String, Test>,
        number: Field<u64, Test>,
        emails: Field<usize, Test>,
    }
    
    use language::{Stemmer, LowercaseFilter, IndexerFunnel, WhitespaceTokenizer};
    use language::integers::NumberFilter;

    #[test]
    fn test() {
        use perlin_core::index::posting::Posting;
        let mut t = Test::create(
            &create_test_dir("doc_index/test"),
            Some(pipeline!(Test: text
                           WhitespaceTokenizer
                           > NumberFilter
                           | [number]
                           > LowercaseFilter
                           > Stemmer(Algorithm::English))),
            None, None);

        t.index_field(DocId(0), "text", "10 birds flew over MT EVEREST");
        t.index_field(DocId(1), "text", "125 birds flew accross THE ocean");
        t.index_field(DocId(2), "text", "2514 unicorns drew a CAR on mars");
        t.commit();
        assert_eq!(t.text.index.query_atom(&"bird".to_string()), vec![Posting(DocId(0)), Posting(DocId(1))]);
        assert_eq!(t.text.index.query_atom(&"unicorn".to_string()), vec![Posting(DocId(2))]);
        assert_eq!(t.number.index.query_atom(&2514), vec![Posting(DocId(2))]);        
    }
}
