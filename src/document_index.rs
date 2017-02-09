use std::hash::Hash;
use std::path::{PathBuf, Path};
use std::str::FromStr;

use std::collections::BTreeMap;

use perlin_core::index::Index;
use perlin_core::index::vocabulary::SharedVocabulary;
use perlin_core::index::posting::{Posting, DocId};
use perlin_core::page_manager::{RamPageCache, FsPageManager};

use field::{FieldDefinition, Field, FieldId};
use language::pipeline::PipelineElement;

/// `DocumentIndex` takes some of the basic building blocks in `perlin_core`
/// and provides an abstraction that can be used to index and query documents
/// using fields, metadata, taxonomies etc
pub struct DocumentIndex<TContainer> {
    // We need to overwrite perlin_core's default DocIds as some Documents might contain
    // other fields than others. This counter acts as DocumentIndex global document counter.
    doc_id_counter: DocId,
    // The base path of this index.
    base_path: PathBuf,
    // A, possible shared between many DocumentIndices, vocabulary for string-terms
    vocabulary: SharedVocabulary<String>,
    index_container: TContainer,
    pipelines: BTreeMap<FieldId, Box<PipelineElement<TContainer>>>,

}

pub trait IndexContainer<T: Hash + Eq> {
    fn manage_index(&mut self, FieldDefinition, Index<T>);
}

pub trait Commitable {
    fn commit(&mut self);
}

pub trait TermIndexer<T> {
    fn index_term(&mut self, Field<T>);
}

pub trait TermQuerier<T> {
    fn query_term(&self, &Field<T>) -> Vec<Posting>;
}

pub trait PipelineProvider {
    fn get_pipeline(&self, FieldDefinition) -> &PipelineElement<Self>;
}

impl<TContainer: Default + Commitable> DocumentIndex<TContainer> {
    /// Create a new index.
    pub fn new(path: &Path, vocab: SharedVocabulary<String>) -> Self {
        DocumentIndex {
            vocabulary: vocab,
            doc_id_counter: DocId::none(),
            base_path: path.to_path_buf(),
            index_container: TContainer::default(),
            pipelines: BTreeMap::new()
        }
    }

    pub fn add_field<TTerm: Hash + Eq + Ord>(&mut self,
                                             field_def: FieldDefinition,
                                             pipeline: Box<PipelineElement<TContainer>>)
                                             -> Result<(), ()>
        where TContainer: IndexContainer<TTerm>
    {
        let FieldDefinition(field_id, _) = field_def;
        let page_cache = RamPageCache::new(FsPageManager::new(&self.base_path
            .join(format!("{}_pages.bin", field_id.0).to_string())));
        self.index_container.manage_index(field_def,
                                          Index::<TTerm>::new(page_cache, SharedVocabulary::new()));
        self.pipelines.insert(field_id, pipeline);
        Ok(())
    }

    pub fn get_next_doc_id(&mut self) -> DocId {
        self.doc_id_counter.inc();
        self.doc_id_counter
    }
    
    /// Indexes a field
    pub fn index_field(&mut self, doc_id: DocId, field: FieldDefinition, content: &str)
    {
        if let Some(pipe) = self.pipelines.get(&field.0) {
            pipe.apply(content, &mut self.index_container);
        } else {
            panic!();
        }
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

    use test_utils::create_test_dir;
    use document::DocumentBuilder;
    use field::{FieldDefinition, FieldType, FieldId, Field};
    use language::{StringFunnel, WhitespaceTokenizer, PipelineElement, CanAppend};
    use language::pipeline;
    
    use std::collections::BTreeMap;

    use perlin_core::index::vocabulary::SharedVocabulary;
    use perlin_core::index::posting::{Posting, DocId};
    use perlin_core::index::Index;

    struct TestContainer {
        cur_doc_id: DocId,
        text_fields: BTreeMap<FieldId, Index<String>>,
        num_fields: BTreeMap<FieldId, Index<u64>>,
    }

    impl IndexContainer<String> for TestContainer {
        fn manage_index(&mut self,
                        def: FieldDefinition,
                        index: Index<String>) {
            self.text_fields.insert(def.0, index);
        }
    }


    impl IndexContainer<u64> for TestContainer {
        fn manage_index(&mut self,
                        def: FieldDefinition,
                        index: Index<u64>) {
            self.num_fields.insert(def.0, index);
        }
    }

    impl TermIndexer<String> for TestContainer {
        fn index_term(&mut self, field: Field<String>) {
            if let Some(index) = self.text_fields.get_mut(&(field.0).0) {
                index.index_term(field.1, self.cur_doc_id);
            }
        }
    }
    
    impl TermIndexer<u64> for TestContainer {
        fn index_term(&mut self, field: Field<u64>) {
            if let Some(index) = self.num_fields.get_mut(&(field.0).0) {
                index.index_term(field.1, self.cur_doc_id);
            }
        }
    }

    impl TermQuerier<String> for TestContainer {
        fn query_term(&self, field: &Field<String>) -> Vec<Posting>{
            if let Some(index) = self.text_fields.get(&(field.0).0) {
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
                cur_doc_id: DocId(0),
                text_fields: BTreeMap::new(),
                num_fields: BTreeMap::new(),
            }
        }
    }

    fn new_index(name: &str) -> DocumentIndex<TestContainer> {
        DocumentIndex::new(&create_test_dir(format!("perlin_index/{}", name).as_str()),
                           SharedVocabulary::new())
    }

    fn get_testpipeline(field_def: FieldDefinition) -> Box<PipelineElement<TestContainer>> {
        Box::new(pipeline!(StringFunnel::new(field_def), WhitespaceTokenizer<TestContainer>))
    }

    #[test]
    fn pipeline_mode() {
        let mut index = new_index("pipeline_mode");
        let text_field_def = FieldDefinition(FieldId(0), FieldType::Text);
        index.add_field::<String>(text_field_def, get_testpipeline(text_field_def));

        index.index_field(DocId(0), text_field_def, "this is a test");
        index.commit();
        assert_eq!(index.query_field(&Field(text_field_def, "test".to_owned())), vec![DocId(0)]);
    }
    
    // #[test]
    // fn one_document() {
    //     let mut index = new_index("one_document");
    //     let text_field_def = FieldDefinition(FieldId(0), FieldType::Text);
    //     // Add a field
    //     index.add_field::<String>(text_field_def);
    //     // Index a new documnet
    //     index.index_field(DocId(0), Field(text_field_def, "title".to_string()));
    //     // Commit the index.
    //     index.commit();
    //     assert_eq!(index.query_field(&Field(text_field_def, "title".to_string())),
    //                vec![DocId(0)]);
    // }

    // #[test]
    // fn multiple_documents() {
    //     let mut index = new_index("multiple_documents");
    //     let text_field_def = FieldDefinition(FieldId(0), FieldType::Text);
    //     // Add a field
    //     index.add_field::<String>(FieldDefinition(FieldId(0), FieldType::Text));
    //     // Index a new documnet
    //     index.index_field(DocId(0), Field(text_field_def, "This is a test title"));
    //     index.index_field(DocId(1), Field(text_field_def, "This is a test text"));

    //     // Commit the index.
    //     index.commit();
    //     assert_eq!(index.query_field(&Field(text_field_def, "title")),
    //                vec![DocId(0)]);
    //     assert_eq!(index.query_field(&Field(text_field_def, "test")),
    //                vec![DocId(0), DocId(1)]);
    // }

    // #[test]
    // fn multiple_fields() {
    //     let mut index = new_index("multiple_fields");
    //     let text_field0 = FieldDefinition(FieldId(0), FieldType::Text);
    //     let text_field1 = FieldDefinition(FieldId(1), FieldType::Text);
    //     index.add_field::<String>(text_field0);
    //     index.add_field::<String>(text_field1);

    //     index.index_field(DocId(0), Field(text_field0, "This is a test title"));
    //     index.index_field(DocId(0), Field(text_field1, "This is a test content"));


    //     index.index_field(DocId(1), Field(text_field0, "This is a test title"));
    //     index.index_field(DocId(1), Field(text_field1, "This is a test content"));

    //     index.commit();
    //     assert_eq!(index.query_field(&Field(text_field0, "content")), vec![]);
    //     assert_eq!(index.query_field(&Field(text_field0, "title")),
    //                vec![DocId(0), DocId(1)]);
    //     assert_eq!(index.query_field(&Field(text_field1, "content")),
    //                vec![DocId(0), DocId(1)]);
    //     assert_eq!(index.query_field(&Field(text_field1, "title")), vec![])
    // }

    // #[test]
    // fn multiple_fieldtypes() {
    //     let mut index = new_index("multiple_fieldtypes");
    //     let text_field0 = FieldDefinition(FieldId(0), FieldType::Text);
    //     let text_field1 = FieldDefinition(FieldId(1), FieldType::Text);
    //     // Planet Number in solar system
    //     let num_field2 = FieldDefinition(FieldId(2), FieldType::Number);
    //     // Object type. 1=star 2=planet 3=moon
    //     let num_field3 = FieldDefinition(FieldId(3), FieldType::Number);
    //     index.add_field::<String>(text_field0);
    //     index.add_field::<String>(text_field1);
    //     index.add_field::<u64>(num_field2);
    //     index.add_field::<u64>(num_field3);
    //     // Mars
    //     index.index_field(DocId(0), Field(text_field0, "Mars"));
    //     index.index_field(DocId(0),
    //                       Field(text_field1,
    //                             "Mars is the fourth planet from the Sun and the second-smallest \
    //                              planet in the Solar System, after Mercury."));
    //     index.index_field(DocId(0), Field(num_field2, 4));
    //     index.index_field(DocId(0), Field(num_field3, 2));
    //     // Sun
    //     index.index_field(DocId(1), Field(text_field0, "Sun"));
    //     index.index_field(DocId(1),
    //                       Field(text_field1,
    //                             "The Sun is the star at the center of the Solar System."));
    //     index.index_field(DocId(1), Field(num_field3, 1));

    //     // Moon

    //     index.index_field(DocId(2), Field(text_field0, "Moon"));
    //     index.index_field(DocId(2),
    //                       Field(text_field1,
    //                             "The Moon is an astronomical body that orbits planet Earth, \
    //                              being Earth's only permanent natural satellite."));
    //     index.index_field(DocId(2), Field(num_field3, 3));

    //     index.commit();
    //     assert_eq!(index.query_field(&Field(text_field0, "Moon")),
    //                vec![DocId(2)]);
    //     assert_eq!(index.query_field(&Field(num_field2, 4)), vec![DocId(0)]);
    //     assert_eq!(index.query_field(&Field(num_field3, 1)), vec![DocId(1)]);
    //     assert_eq!(index.query_field(&Field(text_field0, "is")), vec![]);
    //     assert_eq!(index.query_field(&Field(text_field1, "is")),
    //                vec![DocId(0), DocId(1), DocId(2)]);

    // }
}
