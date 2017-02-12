use std::hash::Hash;
use std::path::{PathBuf, Path};

use perlin_core::index::Index;
use perlin_core::index::vocabulary::SharedVocabulary;
use perlin_core::index::posting::{Posting, DocId};
use perlin_core::page_manager::{RamPageCache, FsPageManager};

use language::CanApply;

use field::{FieldDefinition, Field, FieldId};

type Pipeline<T, Out> = Box<Fn(DocId, FieldId) -> Box<for<'r> CanApply<&'r str, T, Output=Out>>>;

/// `DocumentIndex` takes some of the basic building blocks in `perlin_core`
/// and provides an abstraction that can be used to index and query documents
/// using fields, metadata, taxonomies etc
pub struct DocumentIndex<TContainer, TPipelineContainer> {
    // We need to overwrite perlin_core's default DocIds as some Documents might contain
    // other fields than others. This counter acts as DocumentIndex global document counter.
    doc_id_counter: DocId,
    // The base path of this index.
    base_path: PathBuf,
    index_container: TContainer,
    pipeline_container: TPipelineContainer
}

pub trait IndexContainer<T: Hash + Eq> {
    fn manage_index(&mut self, FieldDefinition, Index<T>);
}

pub trait PipelineContainer<TContainer, TOutput> {
    fn manage_pipeline(&mut self, FieldDefinition, Pipeline<TContainer, TOutput>);
    fn apply_pipeline(&mut self, &mut TContainer, FieldDefinition, DocId, &str);    
}

pub trait Commitable {
    fn commit(&mut self);
}

pub trait TermIndexer<T> {
    fn index_term(&mut self, FieldId, DocId, T);
}

pub trait TermQuerier<T> {
    fn query_term(&self, &Field<T>) -> Vec<Posting>;
}

impl<TContainer: Default + Commitable, TPipelineContainer: Default> DocumentIndex<TContainer, TPipelineContainer> {
    /// Create a new index.
    pub fn new(path: &Path) -> Self {
        DocumentIndex {
            doc_id_counter: DocId::none(),
            base_path: path.to_path_buf(),
            index_container: TContainer::default(),
            pipeline_container: TPipelineContainer::default(),
        }
    }

    pub fn add_field<TTerm: Hash + Eq + Ord>(&mut self,
                                             field_def: FieldDefinition,
                                             pipeline: Pipeline<TContainer, TTerm>)
                                             -> Result<(), ()>
        where TContainer: IndexContainer<TTerm>,
              TPipelineContainer: PipelineContainer<TContainer, TTerm>
    {
        let FieldDefinition(field_id, _) = field_def;
        let page_cache = RamPageCache::new(FsPageManager::new(&self.base_path
            .join(format!("{}_pages.bin", field_id.0).to_string())));
        self.index_container.manage_index(field_def,
                                          Index::<TTerm>::new(page_cache, SharedVocabulary::new()));
        self.pipeline_container.manage_pipeline(field_def,
                                                pipeline);
        Ok(())
    }


    /// Indexes a field
    pub fn index_field<TTerm>(&mut self, doc_id: DocId, field: FieldDefinition, content: &str)
        where TPipelineContainer: PipelineContainer<TContainer, TTerm>
    {
        self.pipeline_container.apply_pipeline(&mut self.index_container,field, doc_id, content);
    }
    
    pub fn get_next_doc_id(&mut self) -> DocId {
        self.doc_id_counter.inc();
        self.doc_id_counter
    }

    /// Commits this index
    pub fn commit(&mut self) {
        self.index_container.commit();
    }

    /// Runs an atom_query on a certain field
    pub fn query_field<TTerm: Hash + Eq>(&self, query: &Field<TTerm>) -> Vec<DocId>
        where TContainer: TermQuerier<TTerm>
    {
        self.index_container
            .query_term(query)
            .into_iter()
            .map(|posting| posting.doc_id())
            .collect()
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    
    use rust_stemmers;

    use test_utils::create_test_dir;
    use field::{FieldDefinition, FieldType, FieldId, Field};
    use language::*;
    use language::integers::*;
    
    use std::collections::BTreeMap;

    use perlin_core::index::posting::{Posting, DocId};
    use perlin_core::index::Index;


    struct TestPipelineContainer {
        text_pipelines: BTreeMap<FieldId, Pipeline<TestContainer, String>>,
        num_pipelines: BTreeMap<FieldId, Pipeline<TestContainer, u64>>
    }

    impl Default for TestPipelineContainer {
        fn default() -> Self {
            TestPipelineContainer {
                text_pipelines: BTreeMap::new(),
                num_pipelines: BTreeMap::new()
            }
        }
    }

    impl PipelineContainer<TestContainer, String> for TestPipelineContainer {
        fn manage_pipeline(&mut self, def: FieldDefinition, pipe: Pipeline<TestContainer, String>) {
            self.text_pipelines.insert(def.0, pipe);
        }

        fn apply_pipeline(&mut self, cont: &mut TestContainer, def: FieldDefinition, doc_id: DocId, input: &str) {
            if let Some(pipe) = self.text_pipelines.get(&def.0) {
                pipe(doc_id, def.0).apply(input, cont);
            }
        }
    }

    impl PipelineContainer<TestContainer, u64> for TestPipelineContainer {
        fn manage_pipeline(&mut self, def: FieldDefinition, pipe: Pipeline<TestContainer, u64>) {
            self.num_pipelines.insert(def.0, pipe);
        }

        fn apply_pipeline(&mut self, cont: &mut TestContainer, def: FieldDefinition, doc_id: DocId, input: &str) {
            if let Some(pipe) = self.num_pipelines.get(&def.0) {
                pipe(doc_id, def.0).apply(input, cont);
            }
        }
    }
    
    
    struct TestContainer {    
        text_fields: BTreeMap<FieldId, Index<String>>,
        num_fields: BTreeMap<FieldId, Index<u64>>,
    }

    impl IndexContainer<String> for TestContainer {
        fn manage_index(&mut self, def: FieldDefinition, index: Index<String>) {
            self.text_fields.insert(def.0, index);
        }
    }


    impl IndexContainer<u64> for TestContainer {
        fn manage_index(&mut self, def: FieldDefinition, index: Index<u64>) {
            self.num_fields.insert(def.0, index);
        }       
    }

    impl TermIndexer<String> for TestContainer {
        fn index_term(&mut self, field: FieldId, doc_id: DocId, term: String) {
            if let Some(index) = self.text_fields.get_mut(&field) {
                index.index_term(term, doc_id);
            }
        }
    }

    impl TermIndexer<u64> for TestContainer {
        fn index_term(&mut self, field: FieldId, doc_id: DocId, term: u64) {
            if let Some(index) = self.num_fields.get_mut(&field) {
                index.index_term(term, doc_id);
            }
        }
    }

    impl TermQuerier<String> for TestContainer {
        fn query_term(&self, field: &Field<String>) -> Vec<Posting> {
            if let Some(index) = self.text_fields.get(&(field.0).0) {
                index.query_atom(&field.1)
            } else {
                panic!();
            }
        }        
    }

    impl TermQuerier<u64> for TestContainer {
        fn query_term(&self, field: &Field<u64>) -> Vec<Posting> {
            if let Some(index) = self.num_fields.get(&(field.0).0) {
                index.query_atom(&field.1)
            } else {
                panic!();
            }
        }
    }


    impl Commitable for TestContainer {
        fn commit(&mut self) {
            self.text_fields.values_mut().map(|index| index.commit()).count();
            self.num_fields.values_mut().map(|index| index.commit()).count();
        }
    }


    impl Default for TestContainer {
        fn default() -> Self {
            TestContainer {
                text_fields: BTreeMap::new(),
                num_fields: BTreeMap::new(),
            }
        }
    }

    fn new_index(name: &str) -> DocumentIndex<TestContainer, TestPipelineContainer> {
        DocumentIndex::new(&create_test_dir(format!("perlin_index/{}", name).as_str()))
    }

    #[test] 
    fn pipeline_mode() {
        let mut index = new_index("pipeline_mode");
        let text_field_def = FieldDefinition(FieldId(0), FieldType::Text);
        index.add_field::<String>(text_field_def,
                                  pipeline!( WhitespaceTokenizer
                                             > LowercaseFilter                                             
                                             > Stemmer(rust_stemmers::Algorithm::English)));
                                      
 
        index.index_field::<String>(DocId(0), text_field_def, "this is a TEST");
        index.index_field::<String>(DocId(1), text_field_def, "THIS is a title");
        index.commit();
        assert_eq!(index.query_field(&Field(text_field_def, "test".to_owned())),
                   vec![DocId(0)]);
        assert_eq!(index.query_field(&Field(text_field_def, "titl".to_owned() /*Because it is stemmed!*/)),
                   vec![DocId(1)]);
        assert_eq!(index.query_field(&Field(text_field_def, "this".to_owned())),
                   vec![DocId(0), DocId(1)]);
    }

    #[test]
    fn different_pipelines() {
        let mut index = new_index("different_pipelines");
        let text_def1 = FieldDefinition(FieldId(0), FieldType::Text);
        let text_def2 = FieldDefinition(FieldId(1), FieldType::Text);
        index.add_field::<String>(text_def1,
                                  pipeline!( WhitespaceTokenizer
                                             > LowercaseFilter
                                             > Stemmer(rust_stemmers::Algorithm::English)));
        index.add_field::<String>(text_def2,
                                  pipeline!( WhitespaceTokenizer > LowercaseFilter ));
        index.index_field::<String>(DocId(0), text_def1, "THIS is a title");
        index.index_field::<String>(DocId(1), text_def1, "this is a titles");
        index.index_field::<String>(DocId(0), text_def2, "THIS is a title");
        index.index_field::<String>(DocId(1), text_def2, "this is a titles");
        index.commit();
        assert_eq!(index.query_field(&Field(text_def1, "titl".to_owned())),
                   vec![DocId(0), DocId(1)]);
        assert_eq!(index.query_field(&Field(text_def2, "title".to_owned())),
                   vec![DocId(0)]);        
    }

    #[test]
    fn multityped_pipeline() {
        let mut index = new_index("multityped_pipelines");
        let text_def1 = FieldDefinition(FieldId(0), FieldType::Text);
        let num_def2 = FieldDefinition(FieldId(1), FieldType::Number);
        index.add_field::<String>(text_def1,
                                  pipeline!( WhitespaceTokenizer
                                             > NumberFilter
                                             | [FieldId(1)]
                                             > LowercaseFilter )).unwrap();
        index.add_field::<u64>(num_def2, pipeline!( ToU64 )).unwrap();
        index.index_field::<String>(DocId(0), text_def1, "These are 10 tests");
        index.index_field::<String>(DocId(1), text_def1, "this is 1 single title");
        index.index_field::<u64>(DocId(2), num_def2, "15");
        index.commit();
        assert_eq!(index.query_field(&Field(text_def1, "this".to_owned())),
                   vec![DocId(1)]);
        assert_eq!(index.query_field(&Field(text_def1, "10".to_owned())),
                   vec![]);
        assert_eq!(index.query_field(&Field(num_def2, 10)),
                   vec![DocId(0)]);
        assert_eq!(index.query_field(&Field(num_def2, 15)),
                   vec![DocId(2)]);
    }
}
