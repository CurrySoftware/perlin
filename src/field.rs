use std::sync::Arc;
    
use perlin_core::index::Index;

pub type Field<T> = Index<T>;

pub type FacetField<T> = Index<Arc<T>>;
