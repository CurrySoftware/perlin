use std::path::{PathBuf, Path};

use perlin_core::index::Index;
use perlin_core::index::vocabulary::SharedVocabulary;
use perlin_core::index::posting::DocId;
use perlin_core::page_manager::{RamPageCache, FsPageManager};

use field::{Field, FieldId, FieldContent};
use document::Document;

/// `DocumentIndex` takes some of the basic building blocks in perlin_core
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
        };
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
            _ => {}
        }
    }

    /// Commits this index
    pub fn commit(&mut self) {
        self.string_fields.iter_mut().map(|&mut (_, ref mut index)| index.commit()).count();
        self.number_fields.iter_mut().map(|&mut (_, ref mut index)| index.commit()).count();
    }

    /// Runs a query on this index
    pub fn query_index(&self, query: String) -> Vec<DocId> {
        self.string_fields
            .first()
            .unwrap()
            .1
            .query_atom(&query)
            .into_iter()
            .map(|posting| posting.doc_id())
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::DocumentIndex;

    use test_utils::create_test_dir;
    use document::DocumentBuilder;
    use field::FieldId;

    use perlin_core::index::vocabulary::SharedVocabulary;

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
        assert_eq!(index.query_index("test".to_string()), vec![0]);
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
        assert_eq!(index.query_index("test".to_string()), vec![0, 1]);
    }
}
