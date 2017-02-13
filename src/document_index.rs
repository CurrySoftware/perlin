use std::hash::Hash;
use std::path::{PathBuf, Path};
use std::marker::PhantomData;

use perlin_core::index::Index;
use perlin_core::index::vocabulary::SharedVocabulary;
use perlin_core::index::posting::{Posting, DocId};
use perlin_core::page_manager::{RamPageCache, FsPageManager};

use language::CanApply;

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

    use test_utils::create_test_dir;

//    #[derive(PerlinDocument)]
    struct Test {
        t: Field<String, Test>
    }

  use std::path::Path;
        impl Test {
            pub fn create(path: &Path, t: Option<Pipeline<String, Test>>) -> Self {
                use perlin_core::page_manager::{RamPageCache, FsPageManager};
                let t_page_cache =
                    RamPageCache::new(FsPageManager::new(&path.join("t_page_cache")));
                Test { t: Field::create(t_page_cache, t) }
            }
        }
        impl PerlinDocument for Test {
            fn commit(&mut self) {
                self.t.index.commit();
            }
            fn index_field(&mut self, doc_id: DocId, field_name: &str, field_contents: &str) {
                let pipe = match field_name {
                    "t" => {
                        if let Some(ref pipeline) = self.t.pipeline {
                            pipeline()
                        } else {
                            {
                               panic!()
                            }
                        }
                    }
                    _ => {
                            panic!()                        
                    }
                };
                pipe(doc_id, self, field_contents);
            }
        }

    
    use language::{LowercaseFilter, IndexerFunnel, WhitespaceTokenizer};
    

    #[test]
    fn test() {
        use perlin_core::index::posting::Posting;
        let mut t = Test::create(
            &create_test_dir("doc_index/test"),
            Some(pipeline!(Test: WhitespaceTokenizer
                               > LowercaseFilter )));

        t.index_field(DocId(0), "t", "hans WAR ein GrüßeEndef Vogl");
        t.commit();
        assert_eq!(t.t.index.query_atom(&"hans".to_string()), vec![Posting(DocId(0))]);
    }
}
