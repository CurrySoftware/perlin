use std::sync::Arc;
use std::hash::Hash;
    
use perlin_core::index::Index;
use perlin_core::index::posting::DocId;

use language::PipelineBucket;

pub type Field<T> = Index<T>;

pub type FacetField<T> = Index<Arc<T>>;


impl<TTerm> PipelineBucket<TTerm> for Field<TTerm>
    where TTerm: Hash + Eq + Ord
{
    fn put(&mut self, doc_id: DocId, term: TTerm) {
        self.index_term(doc_id, term)
    }
}
