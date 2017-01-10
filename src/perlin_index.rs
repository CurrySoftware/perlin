use std::path::{PathBuf, Path};

use perlin_core::index::Index;
use perlin_core::index::vocabulary::SharedVocabulary;
use perlin_core::index::posting::DocId;
use perlin_core::page_manager::{RamPageCache, FsPageManager};

use document::{Field, FieldId, FieldContent, Document};

/// `PerlinIndex`es are Indexes provided by perlin_core and specialised for use
/// with text.
/// They also contain auxiliary data structures like an external key storage
pub struct PerlinIndex {
    doc_id_counter: DocId,
    base_path: PathBuf,
    vocabulary: SharedVocabulary<String>,
    string_fields: Vec<(FieldId, Index<String>)>,
    number_fields: Vec<(FieldId, Index<u64>)>,
    external_keys: Vec<(DocId, usize)>,
}

impl PerlinIndex {
    /// Create a new index.
    pub fn new(path: &Path, vocab: SharedVocabulary<String>) -> Self {
        PerlinIndex {
            vocabulary: vocab,
            doc_id_counter: DocId::none(),
            base_path: path.to_path_buf(),
            string_fields: Vec::new(),
            number_fields: Vec::new(),
            external_keys: Vec::new(),
        }
    }

    /// Adds a document to this index
    pub fn add_document(&mut self, document: Document) {
        self.doc_id_counter.inc();
        let doc_id = self.doc_id_counter;
        for field in document.fields.into_iter() {
            self.add_field(doc_id, field);
        }
        self.external_keys.push((doc_id, document.external_id));
    }


    /// Adds an index for a field if it is nowhere to be found!
    fn add_field(&mut self, doc_id: DocId, field: Field) {
        let Field(field_id, field_content) = field;
        match field_content {
            FieldContent::String(content) => {
                let pos = match self.string_fields
                    .binary_search_by_key(&field_id, |&(f_id, _)| f_id) {
                    Err(pos) => {
                        self.string_fields
                            .insert(pos,
                                    (field_id,
                                     Index::new(RamPageCache::new(FsPageManager::new(&self.base_path
                                                    .join(format!("{}_pages.bin", field_id.0)
                                                        .to_string()))),
                                                self.vocabulary.clone())));
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
    pub fn query_index(&self, query: String) -> Vec<usize> {
        self.transform_keys(self.string_fields
            .first()
            .unwrap()
            .1
            .query_atom(&query)
            .into_iter()
            .map(|posting| posting.doc_id()))
    }

    /// When querying a perlin index it will respond with internal doc_ids
    /// What we are external doc ids. Therefore transform them!
    fn transform_keys<TResultIter>(&self, mut results: TResultIter) -> Vec<usize>
        where TResultIter: Iterator<Item = DocId>
    {
        let mut external_results = Vec::new();
        while let Some(doc_id) = results.next() {
            match self.external_keys.binary_search_by_key(&doc_id, |&(d_id, _)| d_id) {
                Ok(index) => external_results.push(self.external_keys[index].1),
                _ => continue,
            }
        }
        external_results
    }
}

#[cfg(test)]
mod tests {
    use super::PerlinIndex;

    use test_utils::create_test_dir;
    use document::{FieldId, DocumentBuilder};

    use perlin_core::index::vocabulary::SharedVocabulary;

    fn new_index(name: &str) -> PerlinIndex {
        PerlinIndex::new(&create_test_dir(format!("perlin_index/{}", name).as_str()),
                         SharedVocabulary::new())
    }

    #[test]
    fn one_document() {
        let mut index = new_index("one_document");
        index.add_document(DocumentBuilder::new(25)
            .add_string_field(FieldId(0), "This is a test title".to_string())
            .build());
        index.commit();
        assert_eq!(index.query_index("test".to_string()), vec![25]);
    }

    #[test]
    fn multiple_documents() {
        let mut index = new_index("multiple_documents");
        index.add_document(DocumentBuilder::new(25)
            .add_string_field(FieldId(0), "This is a test title".to_string())
            .build());
        index.add_document(DocumentBuilder::new(15)
            .add_string_field(FieldId(0), "This is a test title".to_string())
            .build());
        index.commit();
        assert_eq!(index.query_index("test".to_string()), vec![25, 15]);
    }
}
