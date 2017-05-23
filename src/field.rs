use std::hash::Hash;
use std::ops::{Deref, DerefMut};
use std::collections::HashMap;

use perlin_core::index::Index;
use perlin_core::index::posting::DocId;
use perlin_core::index::vocabulary::TermId;

use language::PipelineBucket;

pub type Field<T> = Index<T>;

pub struct FilterField<T: Hash + Eq> {
    pub sorted_terms: Vec<(usize, T, TermId)>,
    pub index: Index<T>,
}

impl<T: Hash + Eq + Ord + Clone + 'static> FilterField<T> {
    pub fn commit(&mut self) {
        self.index.commit();

        let mut sorted_terms: Vec<(usize, T, TermId)> = self.index
            .iterate_terms()
            .map(|(t, term_id)| (self.index.term_df(term_id), t.clone(), *term_id))
            .collect::<Vec<_>>();

        sorted_terms.sort_by(|a, b| a.0.cmp(&b.0).reverse());
        self.sorted_terms = sorted_terms;
    }

    pub fn frequent_terms<'a>(&'a self) -> Box<Iterator<Item = (usize, &T, TermId)> + 'a> {
                    Box::new(self.sorted_terms
                             .iter()
                             .map(move |&(ref df, ref t, ref term_id)| (*df, t, *term_id)))
    }
        

    pub fn new(index: Index<T>) -> Self {
        FilterField {
            index,
            sorted_terms: vec![]
        }
    }
}



impl<T: Hash + Eq> DerefMut for FilterField<T> {
    fn deref_mut(&mut self) -> &mut Index<T> {
        &mut self.index
    }
}

impl<T: Hash + Eq> Deref for FilterField<T> {
    type Target = Index<T>;
    fn deref(&self) -> &Index<T> {
        &self.index
    }
}


pub struct HierarchyField<T: Hash + Eq> {
    pub hierarchy: Hierarchy<T>,
    pub index: Index<T>,
}

pub struct Hierarchy<T>(HashMap<T, Vec<T>>, Vec<T>);

impl<T: Hash + Eq + Clone> Hierarchy<T> {
    pub fn new() -> Self {
        Hierarchy(HashMap::new(), vec![])
    }

    pub fn add_element(&mut self, term: T, parent: Option<T>) {
        if self.0.contains_key(&term) {
            panic!("Hierarchy element already exists!");
        }

        self.0.insert(term.clone(), vec![]).unwrap();

        if let Some(parent) = parent {
            if let Some(parent_node) = self.0.get_mut(&parent) {
                parent_node.push(term);
            } else {
                panic!("Added hierarchical elements in wrong order!");
            }
        } else {
            self.1.push(term.clone());
        }
    }

    pub fn get_child_terms(&self, term: T) -> Option<&[T]> {
        if let Some(node) = self.0.get(&term) {
            Some(&node)
        } else {
            None
        }
    }

    pub fn get_root_terms(&self) -> &[T] {
        &self.1
    }
}

impl<TTerm> PipelineBucket<TTerm> for Field<TTerm>
    where TTerm: Hash + Eq + Ord
{
    fn put(&mut self, doc_id: DocId, term: TTerm) {
        self.index_term(doc_id, term)
    }
}


//#[cfg(test)]
mod tests {
    use perlin_core::index::posting::DocId;
    use field::{Field, FilterField};

    use rust_stemmers::Algorithm;

    #[derive(PerlinDocument)]
    pub struct FilterTest {
        body: Field<String>,
        number: FilterField<u64>,
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
