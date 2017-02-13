use std::hash::Hash;

use perlin_core::index::Index;
use perlin_core::page_manager::RamPageCache;
use perlin_core::index::vocabulary::SharedVocabulary;

use document::PerlinDocument;
use document_index::Pipeline;

pub struct Field<T: Hash + Eq, TCont>{
    pub index: Index<T>,
    pub pipeline: Option<Pipeline<T, TCont>>
}


impl<T: Hash + Eq + Ord, TCont> Field<T, TCont> {

    /// Creates a new index by giving it a indexing pipeline and a page cache
    pub fn create(page_cache: RamPageCache, pipeline: Option<Pipeline<T, TCont>>) -> Self {
        Field {
            index: Index::new(page_cache, SharedVocabulary::new()),
            pipeline: pipeline
        }
    }
}
