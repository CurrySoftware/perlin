use std::hash::Hash;
use std::path::Path;
use std::ops::{Deref, DerefMut};
use std::collections::HashMap;

use perlin_core::index::Index;
use perlin_core::index::posting::DocId;

use language::PipelineBucket;

mod filter;
mod hierarchy;

pub use field::filter::Filter;
pub use field::hierarchy::Hierarchy;

pub enum FieldSupplement<T> {
    None,
    Filter(Filter<T>),
    Hierarchy(Hierarchy<T>),
}

pub struct Field<T: Hash + Eq> {
    index: Index<T>,
    pub term_doc_ratio: f32,
    pub supplement: FieldSupplement<T>,
}

impl<T: Hash + Eq + Ord + Clone + 'static> Field<T> {
    pub fn commit(&mut self) {
        self.index.commit();
        if let FieldSupplement::Filter(ref mut filter) = self.supplement {
            filter.commit(&self.index);
        }
    }
}

impl<TTerm> PipelineBucket<TTerm> for Field<TTerm>
    where TTerm: Hash + Eq + Ord
{
    fn put(&mut self, doc_id: DocId, term: TTerm) {
        self.index.index_term(doc_id, term);
    }
}



impl<T: Hash + Eq> DerefMut for Field<T> {
    fn deref_mut(&mut self) -> &mut Index<T> {
        &mut self.index
    }
}

impl<T: Hash + Eq> Deref for Field<T> {
    type Target = Index<T>;
    fn deref(&self) -> &Index<T> {
        &self.index
    }
}


pub struct Fields<T: Hash + Eq> {
    pub fields: HashMap<String, Field<T>>,
}

impl<T: Hash + Eq + Ord + Clone + 'static> Fields<T> {
    pub fn commit(&mut self) {
        for field in self.fields.values_mut() {
            (field as &mut Field<T>).commit();
        }
    }

    pub fn add_field(&mut self,
                     name: String,
                     path: &Path,
                     supplement: FieldSupplement<T>)
                     -> Result<(), ()> {
        use perlin_core::page_manager::{RamPageCache, FsPageManager};
        use perlin_core::index::vocabulary::SharedVocabulary;
        use perlin_core::index::Index;
        if self.fields.contains_key(&name) {
            return Err(());
        } else {
            let page_cache =
                RamPageCache::new(FsPageManager::new(&path.join(format!("{}_page_cache", name))));
            self.fields.insert(name,
                               Field {
                                   index: Index::new(page_cache, SharedVocabulary::new()),
                                   term_doc_ratio: 1.0,
                                   supplement,
                               });
            return Ok(());
        }
    }

    pub fn new() -> Self {
        Fields { fields: HashMap::new() }
    }
}


#[cfg(test)]
mod tests {
    use perlin_core::index::posting::DocId;
    use field::{Fields};

    use rust_stemmers::Algorithm;

    #[derive(PerlinDocument)]
    pub struct FilterTest {
        body: Fields<String>,
    }
    // pub use self::perlin_impl::FilterTestIndex;
    // mod perlin_impl {
    //     use super::*;
    //     use std::path::{Path, PathBuf};
    //     use std::borrow::Cow;
    //     use document_index::Pipeline;
    //     use document_index::QueryPipeline;
    //     use document_index::QueryResultIterator;
    //     use query::Operand;
    //     use query::Query;
    //     use perlin_core::index::posting::{PostingIterator, DocId};
    //     use perlin_core::index::vocabulary::TermId;
    //     pub struct FilterTestIndex {
    //         pub documents: FilterTest,
    //         pub query_pipeline: Option<QueryPipeline<FilterTest>>,
    //         pub doc_counter: DocId,
    //     }
    //     impl FilterTestIndex {
    //         pub fn create(base_path: PathBuf) -> Self {
    //             FilterTestIndex {
    //                 documents: FilterTest::create(&base_path),
    //                 query_pipeline: None,
    //                 doc_counter: DocId::none(),
    //             }
    //         }
    //         pub fn commit(&mut self) {
    //             self.documents.commit();
    //         }
    // pub fn set_query_pipeline(&mut self, pipe:
    // QueryPipeline<FilterTest>) {
    //             self.query_pipeline = Some(pipe);
    //         }
    //         pub fn run_query<'a>(&'a self, query: Query<'a>) -> Operand<'a> {
    //             if let Some(ref query_pipe) = self.query_pipeline {
    //                 query_pipe(&self.documents, query)
    //             } else {
    //                 {
    //                     panic!()
    //                 };
    //             }
    //         }
    //     }
    //     impl FilterTest {
    //         pub fn create(path: &Path) -> Self {
    //             use perlin_core::page_manager::{RamPageCache, FsPageManager};
    //             use perlin_core::index::vocabulary::SharedVocabulary;
    //             use perlin_core::index::Index;
    //             let body_page_cache =
    //                 RamPageCache::new(
    // FsPageManager::new(&path.join("body_page_cache")));
    //             let number_page_cache =
    //                 RamPageCache::new(
    // FsPageManager::new(&path.join("number_page_cache")));
    //             FilterTest {
    //                 body: Index::new(body_page_cache, SharedVocabulary::new()),
    // number: FilterField::new(Index::new(number_page_cache,
    // SharedVocabulary::new())),
    //             }
    //         }
    //         pub fn commit(&mut self) {
    //             self.body.commit();
    //             self.number.commit();
    //         }
    //     }
    // }


}
