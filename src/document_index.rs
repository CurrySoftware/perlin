use std::path::{PathBuf, Path};
use std::str::FromStr;

use perlin_core::index::Index;
use perlin_core::index::vocabulary::SharedVocabulary;
use perlin_core::index::posting::DocId;
use perlin_core::page_manager::{RamPageCache, FsPageManager};

use field::{RawField, FieldId, FieldDefinition, FieldType};

use document::Document;

/// `DocumentIndex` takes some of the basic building blocks in `perlin_core`
/// and provides an abstraction that can be used to index and query documents
/// using fields, metadata, taxonomies etc
pub struct DocumentIndex {
    // We need to overwrite perlin_core's default DocIds as some Documents might contain
    // other fields than others. This counter acts as DocumentIndex global document counter.
    doc_id_counter: DocId,
    // The base path of this index.
    base_path: PathBuf,
    // A, possible shared between many DocumentIndices, vocabulary for string-terms
    vocabulary: SharedVocabulary<String>,
    // Indices for fields that contain strings
    text_fields: Vec<(FieldId, Index<String>)>,
    // Indices for fields that contain numbers
    number_fields: Vec<(FieldId, Index<u64>)>,
}

impl DocumentIndex {
    /// Create a new index.
    pub fn new(path: &Path, vocab: SharedVocabulary<String>) -> Self {
        DocumentIndex {
            vocabulary: vocab,
            doc_id_counter: DocId::none(),
            base_path: path.to_path_buf(),
            text_fields: Vec::new(),
            number_fields: Vec::new(),
        }
    }

    /// Adds a document to this index by indexing every field in its index
    pub fn add_document(&mut self, document: Document) -> DocId {
        self.doc_id_counter.inc();
        let doc_id = self.doc_id_counter;
        for field in document.0 {
            self.index_field(doc_id, field);
        }
        doc_id
    }

    pub fn add_field(&mut self, field_def: FieldDefinition) -> Result<(), ()> {
        let FieldDefinition(field_id, field_type) = field_def;
        let page_cache = RamPageCache::new(FsPageManager::new(&self.base_path
            .join(format!("{}_pages.bin", field_id.0).to_string())));
        match field_type {
            FieldType::Text => {
                if let Err(pos) = self.text_fields
                    .binary_search_by_key(&field_id, |&(f_id, _)| f_id) {
                    self.text_fields.insert(pos,
                                            (field_id,
                                             Index::new(page_cache, self.vocabulary.clone())));
                }
            }
            FieldType::Number => {
                if let Err(pos) = self.number_fields
                    .binary_search_by_key(&field_id, |&(f_id, _)| f_id) {
                    self.number_fields.insert(pos,
                                              (field_id,
                                               Index::new(page_cache, SharedVocabulary::new())));
                }
            }
        };
        Ok(())
    }

    /// Indexes a field. Might create a new index!
    fn index_field(&mut self, doc_id: DocId, field: RawField) {
        match field {
            RawField(FieldDefinition(field_id, FieldType::Text), content) => {
                // Find index for field_id
                if let Ok(pos) = self.text_fields
                    .binary_search_by_key(&field_id, |&(f_id, _)| f_id) {
                    self.text_fields[pos].1.index(doc_id, content);
                } else {
                    panic!("Something is seriously wrong!");
                }
            }
            RawField(FieldDefinition(field_id, FieldType::Number), content) => {
                // Find index for field_id
                if let Ok(pos) = self.number_fields
                    .binary_search_by_key(&field_id, |&(f_id, _)| f_id) {
                    self.number_fields[pos].1.index(doc_id, content);
                } else {
                    panic!("Something is seriously wrong!");
                }
            }
        }
    }

    /// Commits this index
    pub fn commit(&mut self) {
        self.text_fields.iter_mut().map(|&mut (_, ref mut index)| index.commit()).count();
        self.number_fields.iter_mut().map(|&mut (_, ref mut index)| index.commit()).count();
    }

    /// Runs an atom_query on a certain field
    pub fn query_field(&self, query: &RawField) -> Vec<DocId> {
        match *query {
            RawField(FieldDefinition(field_id, FieldType::Text), content) => {
                if let Ok(pos) = self.text_fields
                    .binary_search_by_key(&field_id, |&(f_id, _)| f_id) {
                    return self.text_fields[pos].1.query(content);
                } else {
                    panic!("Something is seriously wrong!");
                }
            }
            RawField(FieldDefinition(field_id, FieldType::Number), content) => {
                if let Ok(pos) = self.number_fields
                    .binary_search_by_key(&field_id, |&(f_id, _)| f_id) {
                    return self.number_fields[pos].1.query(content);
                } else {
                    panic!("Something is seriously wrong!");
                }
            }
        }
        vec![]
    }
}




trait Indexer<'a> {
    fn index(&mut self, DocId, &'a str);
    fn query(&self, &'a str) -> Vec<DocId>;
}

impl<'a> Indexer<'a> for Index<String> {
    fn index(&mut self, doc_id: DocId, data: &'a str) {
        self.index_document(data.split_whitespace().map(|s| s.to_string()), Some(doc_id));
    }

    fn query(&self, query: &'a str) -> Vec<DocId> {
        self.query_atom(&query.to_string()).into_iter().map(|posting| posting.doc_id()).collect()
    }
}

impl<'a> Indexer<'a> for Index<u64> {
    fn index(&mut self, doc_id: DocId, data: &'a str) {
        let num = u64::from_str(data).unwrap();
        self.index_document(vec![num].into_iter(), Some(doc_id));
    }

    fn query(&self, query: &'a str) -> Vec<DocId> {
        let num = u64::from_str(query).unwrap();
        self.query_atom(&num).into_iter().map(|posting| posting.doc_id()).collect()
    }
}


#[cfg(test)]
mod tests {
    use super::DocumentIndex;

    use test_utils::create_test_dir;
    use document::DocumentBuilder;
    use field::{FieldDefinition, FieldType, FieldId, RawField};

    use perlin_core::index::vocabulary::SharedVocabulary;
    use perlin_core::index::posting::DocId;

    fn new_index(name: &str) -> DocumentIndex {
        DocumentIndex::new(&create_test_dir(format!("perlin_index/{}", name).as_str()),
                           SharedVocabulary::new())
    }

    #[test]
    fn one_document() {
        let mut index = new_index("one_document");
        let text_field_def = FieldDefinition(FieldId(0), FieldType::Text);
        // Add a field
        index.add_field(FieldDefinition(FieldId(0), FieldType::Text));
        // Index a new documnet
        index.add_document(DocumentBuilder::new()
            .add_field(RawField(text_field_def, "This is a test title"))
            .build());
        // Commit the index.
        index.commit();
        assert_eq!(index.query_field(&RawField(text_field_def, "title")),
                   vec![DocId(0)]);
    }

    #[test]
    fn multiple_documents() {
        let mut index = new_index("multiple_documents");
        let text_field_def = FieldDefinition(FieldId(0), FieldType::Text);
        // Add a field
        index.add_field(FieldDefinition(FieldId(0), FieldType::Text));
        // Index a new documnet
        index.add_document(DocumentBuilder::new()
            .add_field(RawField(text_field_def, "This is a test title"))
            .build());
        index.add_document(DocumentBuilder::new()
            .add_field(RawField(text_field_def, "This is a test text"))
            .build());

        // Commit the index.
        index.commit();
        assert_eq!(index.query_field(&RawField(text_field_def, "title")),
                   vec![DocId(0)]);
        assert_eq!(index.query_field(&RawField(text_field_def, "test")),
                   vec![DocId(0), DocId(1)]);
    }

    #[test]
    fn multiple_fields() {
        let mut index = new_index("multiple_fields");
        let text_field0 = FieldDefinition(FieldId(0), FieldType::Text);
        let text_field1 = FieldDefinition(FieldId(1), FieldType::Text);
        index.add_field(text_field0);
        index.add_field(text_field1);
        index.add_document(DocumentBuilder::new()
            .add_field(RawField(text_field0, "This is a test title"))
            .add_field(RawField(text_field1, "This is a test content"))
            .build());
        index.add_document(DocumentBuilder::new()
            .add_field(RawField(text_field0, "This is a test title"))
            .add_field(RawField(text_field1, "This is a test content"))
            .build());
        index.commit();
        assert_eq!(index.query_field(&RawField(text_field0, "content")), vec![]);
        assert_eq!(index.query_field(&RawField(text_field0, "title")),
                   vec![DocId(0), DocId(1)]);
        assert_eq!(index.query_field(&RawField(text_field1, "content")),
                   vec![DocId(0), DocId(1)]);
        assert_eq!(index.query_field(&RawField(text_field1, "title")), vec![])
    }

    #[test]
    fn multiple_fieldtypes() {
        let mut index = new_index("multiple_fieldtypes");
        let text_field0 = FieldDefinition(FieldId(0), FieldType::Text);
        let text_field1 = FieldDefinition(FieldId(1), FieldType::Text);
        // Planet Number in solar system
        let num_field2 = FieldDefinition(FieldId(2), FieldType::Number);
        // Object type. 1=star 2=planet 3=moon
        let num_field3 = FieldDefinition(FieldId(3), FieldType::Number);
        index.add_field(text_field0);
        index.add_field(text_field1);
        index.add_field(num_field2);
        index.add_field(num_field3);        
        // Mars
        index.add_document(DocumentBuilder::new()
            .add_field(RawField(text_field0, "Mars"))
            .add_field(RawField(text_field1,
                                "Mars is the fourth planet from the Sun and the \
                                 second-smallest planet in the Solar System, after Mercury."))
            .add_field(RawField(num_field2, "4"))
            .add_field(RawField(num_field3, "2"))
            .build());
        // Sun
        index.add_document(DocumentBuilder::new()
            .add_field(RawField(text_field0, "Sun"))
            .add_field(RawField(text_field1,
                                "The Sun is the star at the center of the Solar System."))
            .add_field(RawField(num_field3, "1"))
            .build());

        // Moon
        index.add_document(DocumentBuilder::new()
            .add_field(RawField(text_field0, "Moon"))
            .add_field(RawField(text_field1,
                                "The Moon is an astronomical body that orbits planet Earth, \
                                 being Earth's only permanent natural satellite."))
            .add_field(RawField(num_field3, "3"))
            .build());

        index.commit();
        assert_eq!(index.query_field(&RawField(text_field0, "Moon")),
                   vec![DocId(2)]);
        assert_eq!(index.query_field(&RawField(num_field2, "4")),
                   vec![DocId(0)]);
        assert_eq!(index.query_field(&RawField(num_field3, "1")),
                   vec![DocId(1)]);
        assert_eq!(index.query_field(&RawField(text_field0, "is")),
                   vec![]);
        assert_eq!(index.query_field(&RawField(text_field1, "is")),
                   vec![DocId(0), DocId(1), DocId(2)]);

    }
}
