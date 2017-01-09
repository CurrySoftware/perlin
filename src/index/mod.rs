use std::fmt::Debug;
use std::hash::Hash;
use std::collections::HashMap;

use std::sync::{Arc, RwLock};


use page_manager::RamPageCache;
use index::listing::Listing;
use index::posting::{DocId, Posting};
use index::vocabulary::{Vocabulary, TermId, SharedVocabulary};

pub mod vocabulary;
pub mod posting;
mod listing;

/// Central struct of perlin
/// Stores and manages an index with its listings and vocabulary
pub struct Index<TTerm> {
    page_manager: RamPageCache,
    listings: Vec<(TermId, Listing)>,
    vocabulary: SharedVocabulary<TTerm>,
    last_doc_id: DocId,
    doc_count: usize,
}


impl<TTerm> Index<TTerm>
    where TTerm: Hash + Ord
{
    pub fn new(page_manager: RamPageCache,
               vocabulary: SharedVocabulary<TTerm>)
               -> Self {
        Index {
            page_manager: page_manager,
            listings: Vec::new(),
            vocabulary: vocabulary,
            last_doc_id: DocId::none(),
            doc_count: 0,
        }

    }

    /// Index a whole collection returning the corresponding document_ids
    pub fn index_collection<TDocIter, TDocsIter>(&mut self, collection: TDocsIter) -> Vec<DocId>
        where TDocIter: Iterator<Item = TTerm>,
              TDocsIter: Iterator<Item = TDocIter>
    {
        let mut result = Vec::new();
        let mut buff = Vec::new();
        for doc in collection {
            self.last_doc_id.inc();
            let doc_id = self.last_doc_id;
            result.push(doc_id);
            self.doc_count += 1;
            for term in doc {
                let term_id = self.vocabulary.get_or_add(term);
                buff.push((term_id, doc_id));
            }
        }
        buff.sort_by_key(|&(tid, _)| tid);
        buff.dedup();
        let mut grouped_buff = Vec::with_capacity(buff.len());
        let mut last_tid = TermId(0);
        let mut term_counter = 0;
        for (i, &(term_id, doc_id)) in buff.iter().enumerate() {
            // if term is the first term or different to the last term (new group)
            if last_tid < term_id || i == 0 {
                term_counter += 1;
                // Term_id has to be added
                grouped_buff.push((term_id, vec![Posting(doc_id)]));
                last_tid = term_id;
                continue;
            }
            // Otherwise add a whole new posting
            grouped_buff[term_counter - 1].1.push(Posting(doc_id));
        }
        for (term_id, postings) in grouped_buff {
            let index = match self.listings.binary_search_by_key(&term_id, |&(t_id, _)| t_id) {
                Ok(index) => index,
                Err(index) => {
                    self.listings.insert(index, (term_id, Listing::new()));
                    index
                }
            };
            self.listings[index].1.add(&postings, &mut self.page_manager);
        }
        self.commit();
        result
    }

    /// Index a single document. If this should be retrievable right away, a
    /// call to commit is needed afterwards
    ///
    /// You may overwrite the assigned doc id
    pub fn index_document<TIter>(&mut self,
                                 document: TIter,
                                 overwrite_doc_id: Option<DocId>)
                                 -> DocId
        where TIter: Iterator<Item = TTerm>
    {
        //check if user wants to overwrite doc id.
        //If so, assert, that the one assumption about doc_ids is enforced:
        //They are strictly monotonically increasing.
        //If this is not the case: fail hard before something bad happens!
        let doc_id = if let Some(doc_id) = overwrite_doc_id {
            assert!(doc_id > self.last_doc_id || self.last_doc_id == DocId::none());
            self.last_doc_id = doc_id;
            doc_id
        } else {
            self.last_doc_id.inc();
            self.last_doc_id
        };
        self.doc_count += 1;
        let mut buff = Vec::new();
        for term in document {
            let term_id = self.vocabulary.get_or_add(term);
            buff.push(term_id);
        }
        buff.sort();
        buff.dedup();
        for term_id in buff {
            // get or add listing
            let index = match self.listings.binary_search_by_key(&term_id, |&(t_id, _)| t_id) {
                Ok(index) => index,
                Err(index) => {
                    self.listings.insert(index, (term_id, Listing::new()));
                    index
                }
            };
            self.listings[index].1.add(&[Posting(doc_id)], &mut self.page_manager);
        }
        doc_id
    }

    pub fn commit(&mut self) {
        // We iterate over the listings in reverse here because listing.commit() causes
        // a remove in the ram_page_manager.construction cache which is a Vec.
        // Vec.remove is O(n-i).
        for listing in self.listings.iter_mut().rev() {
            listing.1.commit(&mut self.page_manager);
        }
    }

    pub fn query_atom(&self, atom: &TTerm) -> Vec<Posting>
        where TTerm: Debug
    {
        if let Some(term_id) = self.vocabulary.get(atom) {
            if let Ok(index) = self.listings.binary_search_by_key(&term_id, |&(t_id, _)| t_id) {
                return self.listings[index].1.posting_iter(&self.page_manager).collect::<Vec<_>>();
            }
        }
        vec![]
    }
}


#[cfg(test)]
mod tests {
    use test_utils::create_test_dir;

    use super::Index;
    use index::posting::{Posting, DocId};
    use index::vocabulary::SharedVocabulary;
    use page_manager::{FsPageManager, RamPageCache};

    fn new_index(name: &str) -> Index<usize> {
        let path = &create_test_dir(format!("index/{}", name).as_str());
        let pmgr = FsPageManager::new(&path.join("pages.bin"));
        Index::<usize>::new(RamPageCache::new(pmgr),
                            SharedVocabulary::new())
    }

    #[test]
    fn basic_indexing() {
        let mut index = new_index("basic_indexing");

        assert_eq!(index.index_document((0..2000), None), DocId(0));
        assert_eq!(index.index_document((2000..4000), None), DocId(1));
        assert_eq!(index.index_document((500..600), None), DocId(2));
        index.commit();

        assert_eq!(index.query_atom(&0), vec![Posting(DocId(0))]);
    }

    #[test]
    fn extended_indexing() {
        let mut index = new_index("extended_indexing");
        for i in 0..200 {
            assert_eq!(index.index_document((i..i + 200), None), DocId(i as u64));
        }
        index.commit();

        assert_eq!(index.query_atom(&0), vec![Posting(DocId(0))]);
        assert_eq!(index.query_atom(&99),
                   (0..100).map(|i| Posting(DocId(i))).collect::<Vec<_>>());
    }

    #[test]
    fn collection_indexing() {
        let mut index = new_index("collection_indexing");
        assert_eq!(index.index_collection((0..200).map(|i| (i..i + 200))),
                   (0..200).map(|i| DocId(i)).collect::<Vec<_>>());

        assert_eq!(index.query_atom(&0), vec![Posting(DocId(0))]);
        assert_eq!(index.query_atom(&99),
                   (0..100).map(|i| Posting(DocId(i))).collect::<Vec<_>>());
    }

    #[test]
    fn mutable_index() {
        let mut index = new_index("mutable_index");
        for i in 0..200 {
            assert_eq!(index.index_document((i..i + 200), None), DocId(i as u64));
        }
        index.commit();

        assert_eq!(index.query_atom(&0), vec![Posting(DocId(0))]);
        assert_eq!(index.query_atom(&99),
                   (0..100).map(|i| Posting(DocId(i))).collect::<Vec<_>>());
        assert_eq!(index.index_document(0..400, None), DocId(200));
        index.commit();
        assert_eq!(index.query_atom(&0),
                   vec![Posting(DocId(0)), Posting(DocId(200))]);
    }

    #[test]
    fn shared_vocabulary() {
        let path = &create_test_dir("index/shared_vocabulary");
        let pmgr1 = FsPageManager::new(&path.join("pages1.bin"));
        let pmgr2 = FsPageManager::new(&path.join("pages2.bin"));
        let vocab = SharedVocabulary::new();

        let mut index1 = Index::<usize>::new(RamPageCache::new(pmgr1), vocab.clone());
        let mut index2 = Index::<usize>::new(RamPageCache::new(pmgr2), vocab.clone());

        for i in 0..200 {
            if i % 2 == 0 {
                assert_eq!(index1.index_document((i..i + 200).filter(|i| i % 2 == 0),
                                                 Some(DocId(i as u64))),
                           DocId(i as u64));
            } else {
                assert_eq!(index2.index_document((i..i + 200).filter(|i| i % 2 != 0),
                                                 Some(DocId(i as u64))),
                           DocId(i as u64));
            }
        }
        index1.commit();
        index2.commit();

        assert_eq!(index1.query_atom(&99), vec![]);
        assert_eq!(index2.query_atom(&99),
                   (0..100).filter(|i| i % 2 != 0).map(|i| Posting(DocId(i))).collect::<Vec<_>>());

        assert_eq!(index1.query_atom(&200),
                   (1..200)
                       .filter(|i| i % 2 == 0)
                       .map(|i| Posting(DocId(i)))
                       .collect::<Vec<_>>());
        assert_eq!(index2.query_atom(&200), vec![]);
    }

    #[test]
    #[should_panic]
    fn wrong_overwritten_doc_id() {
        let mut index = new_index("wrong_overwritten_doc_id");
        index.index_document(0..10, Some(DocId(10)));
        index.index_document(0..10, Some(DocId(5)));
    }
}
