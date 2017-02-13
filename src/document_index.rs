use std::hash::Hash;
use std::path::{PathBuf, Path};

use perlin_core::index::Index;
use perlin_core::index::vocabulary::SharedVocabulary;
use perlin_core::index::posting::{Posting, DocId};
use perlin_core::page_manager::{RamPageCache, FsPageManager};

use language::CanApply;

pub type Pipeline<Out, T> = Box<Fn(DocId) -> Box<for<'r> CanApply<&'r str, T, Output=Out>>>;

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

//#[cfg(test)]
mod tests {
    use document::PerlinDocument;
    use document_index::Pipeline;
    use perlin_core::index::posting::DocId;
    use field::Field;

    use test_utils::create_test_dir;

    #[derive(PerlinDocument)]
    struct Test {
        t: Field<String, Test>
    }
   
    use language::{LowercaseFilter, IndexerFunnel, WhitespaceTokenizer};
    

    //#[test]
    fn test() {
        let mut t = Test::create(
            &create_test_dir("doc_index/test"),
            Some(pipeline!(WhitespaceTokenizer
                             > LowercaseFilter )));

        t.index_field(DocId(0), "t", "hans WAR ein GrüßeEndef Vogl");
    }
}
