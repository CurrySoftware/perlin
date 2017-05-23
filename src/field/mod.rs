use std::hash::Hash;

use perlin_core::index::Index;
use perlin_core::index::posting::DocId;

use language::PipelineBucket;

mod filter_field;
mod hierarchy_field;

pub use field::filter_field::FilterField;
pub use field::hierarchy_field::HierarchyField;

pub type Field<T> = Index<T>;

impl<TTerm> PipelineBucket<TTerm> for Field<TTerm>
    where TTerm: Hash + Eq + Ord
{
    fn put(&mut self, doc_id: DocId, term: TTerm) {
        self.index_term(doc_id, term)
    }
}


#[cfg(test)]
mod tests {
    use perlin_core::index::posting::DocId;
    use field::{Field, FilterField, HierarchyField};

    use rust_stemmers::Algorithm;

    #[derive(PerlinDocument)]
    pub struct FilterTest {
        body: Field<String>,
        number: FilterField<u64>,
        cat: HierarchyField<usize>
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
    //         pub fn set_query_pipeline(&mut self, pipe: QueryPipeline<FilterTest>) {
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
    //                 RamPageCache::new(FsPageManager::new(&path.join("body_page_cache")));
    //             let number_page_cache =
    //                 RamPageCache::new(FsPageManager::new(&path.join("number_page_cache")));
    //             FilterTest {
    //                 body: Index::new(body_page_cache, SharedVocabulary::new()),
    //                 number: FilterField::new(Index::new(number_page_cache, SharedVocabulary::new())),
    //             }
    //         }
    //         pub fn commit(&mut self) {
    //             self.body.commit();
    //             self.number.commit();
    //         }
    //     }
    // }


}
