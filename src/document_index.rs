use std::path::{PathBuf, Path};

use perlin_core::index::Index;
use perlin_core::index::vocabulary::SharedVocabulary;
use perlin_core::index::posting::DocId;
use perlin_core::page_manager::{RamPageCache, FsPageManager};

use field::{Field, FieldId, FieldContent, FieldQuery};
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
    string_fields: Vec<(FieldId, Index<String>)>,
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
            string_fields: Vec::new(),
            number_fields: Vec::new(),
        }
    }

    /// Adds a document to this index by indexing every field in its index
    pub fn add_document(&mut self, document: Document) -> DocId {
        self.doc_id_counter.inc();
        let doc_id = self.doc_id_counter;
        for field in document.take_fields() {
            self.index_field(doc_id, field);
        }
        doc_id
    }

    /// Creates a new `Index<String>` for a new field.
    fn create_new_string_field(&mut self, pos: usize, field_id: FieldId) {
        self.string_fields
            .insert(pos,
                    (field_id,
                     Index::new(RamPageCache::new(FsPageManager::new(&self.base_path
                                    .join(format!("{}_pages.bin", field_id.0).to_string()))),
                                self.vocabulary.clone())));
    }

    /// Creates a new `Index<String>` for a new field.
    fn create_new_number_field(&mut self, pos: usize, field_id: FieldId) {
        self.number_fields
            .insert(pos,
                    (field_id,
                     Index::new(RamPageCache::new(FsPageManager::new(&self.base_path
                                    .join(format!("{}_pages.bin", field_id.0).to_string()))),
                                SharedVocabulary::new())));
    }    

    /// Indexes a field. Might create a new index!
    fn index_field(&mut self, doc_id: DocId, field: Field) {
        let Field(field_id, field_content) = field;
        match field_content {
            FieldContent::String(content) => {
                let pos = match self.string_fields
                    .binary_search_by_key(&field_id, |&(f_id, _)| f_id) {
                    Err(pos) => {
                        self.create_new_string_field(pos, field_id);
                        pos
                    }
                    Ok(pos) => pos,
                };
                self.string_fields[pos]
                    .1
                    .index_document(content.split_whitespace().map(|s| s.to_string()),
                                    Some(doc_id));
            }
            FieldContent::Number(content) => {
                let pos = match self.number_fields
                    .binary_search_by_key(&field_id, |&(f_id, _)| f_id) {
                    Err(pos) => {
                        self.create_new_number_field(pos, field_id);
                        pos
                    }
                    Ok(pos) => pos,
                };
                self.number_fields[pos]
                    .1
                    .index_document(vec![content].into_iter(), Some(doc_id));
            }
        }
    }

    /// Commits this index
    pub fn commit(&mut self) {
        self.string_fields.iter_mut().map(|&mut (_, ref mut index)| index.commit()).count();
        self.number_fields.iter_mut().map(|&mut (_, ref mut index)| index.commit()).count();
    }

    /// Runs an atom_query on a certain field
    pub fn query_field(&self, query: &FieldQuery) -> Vec<DocId> {
        let &FieldQuery(Field(field_id, ref field_content)) = query;
        match *field_content {
            FieldContent::String(ref content) => {
                if let Ok(pos) = self.string_fields
                    .binary_search_by_key(&field_id, |&(f_id, _)| f_id) {
                    return self.string_fields[pos]
                        .1
                        .query_atom(content)
                        .into_iter()
                        .map(|posting| posting.doc_id())
                        .collect();
                }
            },
            FieldContent::Number(ref content) => {
                if let Ok(pos) = self.number_fields
                    .binary_search_by_key(&field_id, |&(f_id, _)| f_id) {
                    return self.number_fields[pos]
                        .1
                        .query_atom(content)
                        .into_iter()
                        .map(|posting| posting.doc_id())
                        .collect();
                }
            }
        }
        vec![]
    }
}

#[cfg(test)]
mod tests {
    use super::DocumentIndex;

    use test_utils::create_test_dir;
    use document::DocumentBuilder;
    use field::{FieldId, FieldQuery};

    use perlin_core::index::vocabulary::SharedVocabulary;
    use perlin_core::index::posting::DocId;

    fn new_index(name: &str) -> DocumentIndex {
        DocumentIndex::new(&create_test_dir(format!("perlin_index/{}", name).as_str()),
                           SharedVocabulary::new())
    }

    #[test]
    fn one_document() {
        let mut index = new_index("one_document");
        index.add_document(DocumentBuilder::new()
            .add_string_field(FieldId(0), "This is a test title".to_string())
            .build());
        index.commit();
        assert_eq!(index.query_field(FieldQuery::new_string(FieldId(0), "test".to_string())),
                   vec![DocId(0)]);
    }

    #[test]
    fn multiple_documents() {
        let mut index = new_index("multiple_documents");
        index.add_document(DocumentBuilder::new()
            .add_string_field(FieldId(0), "This is a test title".to_string())
            .build());
        index.add_document(DocumentBuilder::new()
            .add_string_field(FieldId(0), "This is a test title".to_string())
            .build());
        index.commit();
        assert_eq!(index.query_field(FieldQuery::new_string(FieldId(0), "test".to_string())),
                   vec![DocId(0), DocId(1)]);
    }

    #[test]
    fn multiple_fields() {
        let mut index = new_index("multiple_fields");
        index.add_document(DocumentBuilder::new()
            .add_string_field(FieldId(0), "This is a test title".to_string())
            .add_string_field(FieldId(1), "This is a test content".to_string())
            .build());
        index.add_document(DocumentBuilder::new()
            .add_string_field(FieldId(0), "This is a test title".to_string())
            .add_string_field(FieldId(1), "This is a test content".to_string())
            .build());
        index.commit();
        assert_eq!(index.query_field(FieldQuery::new_string(FieldId(0), "content".to_string())),
                   vec![]);
        assert_eq!(index.query_field(FieldQuery::new_string(FieldId(0), "title".to_string())),
                   vec![DocId(0), DocId(1)]);
        assert_eq!(index.query_field(FieldQuery::new_string(FieldId(1), "content".to_string())),
                   vec![DocId(0), DocId(1)]);
        assert_eq!(index.query_field(FieldQuery::new_string(FieldId(1), "title".to_string())),
                   vec![])
    }

    #[test]
    fn multiple_fieldtypes() {
        let mut index = new_index("multiple_fieldtypes");
        index.add_document(DocumentBuilder::new()
            .add_string_field(FieldId(0), "Mars".to_string())
            .add_string_field(FieldId(1),
                              "Mars is the fourth planet from the Sun and the second-smallest \
                               planet in the Solar System, after Mercury."
                              .to_string())
             //Planet Number in solar system
            .add_number_field(FieldId(2), 4)
             //Object type. 1=star 2=planet 3=moon
            .add_number_field(FieldId(3), 2)
                           .build());
        index.add_document(DocumentBuilder::new()
            .add_string_field(FieldId(0), "Sun".to_string())
            .add_string_field(FieldId(1),
                              "The Sun is the star at the center of the Solar System."
                                  .to_string())
            .add_number_field(FieldId(3), 1)
            .build());
        index.add_document(DocumentBuilder::new()
            .add_string_field(FieldId(0), "Moon".to_string())
            .add_string_field(FieldId(1),
                              "The Moon is an astronomical body that orbits planet Earth, being \
                               Earth's only permanent natural satellite."
                                  .to_string())
            .add_number_field(FieldId(3), 3)
            .build());
        index.commit();
        assert_eq!(index.query_field(FieldQuery::new_string(FieldId(0), "Moon".to_string())),
                   vec![DocId(2)]);
        assert_eq!(index.query_field(FieldQuery::new_number(FieldId(2), 4)),
                   vec![DocId(0)]);
        assert_eq!(index.query_field(FieldQuery::new_number(FieldId(3), 1)),
                   vec![DocId(1)]);
        assert_eq!(index.query_field(FieldQuery::new_string(FieldId(0), "is".to_string())),
                   vec![]);
        assert_eq!(index.query_field(FieldQuery::new_string(FieldId(1), "is".to_string())),
                   vec![DocId(0), DocId(1), DocId(2)]);

    }
}
