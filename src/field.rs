use std::hash::Hash;

use perlin_core::index::Index;
use perlin_core::page_manager::RamPageCache;
use perlin_core::index::vocabulary::SharedVocabulary;

use document::PerlinDocument;
use document_index::Pipeline;

pub struct Field<T: Hash + Eq>{
    index: Index<T>,
    pipeline: Option<Pipeline<PerlinDocument, T>>
}


impl<T: Hash + Eq + Ord> Field<T> {

    /// Creates a new index by giving it a indexing pipeline and a page cache
    fn create(page_cache: RamPageCache, pipeline: Option<Pipeline<PerlinDocument, T>>) -> Self {
        Field {
            index: Index::new(page_cache, SharedVocabulary::new()),
            pipeline: pipeline
        }
    }
}
