use std::hash::Hash;
use std::collections::HashMap;

use page_manager::{BlockIter, RamPageCache};
use index::listing::Listing;
use index::posting::{PostingIterator, DocId};
use index::vocabulary::{Vocabulary, TermId};

pub mod vocabulary;
pub mod posting;
mod listing;


pub struct Index<TTerm> {
    page_manager: RamPageCache,
    listings: Vec<(TermId, Listing)>,
    vocabulary: HashMap<TTerm, TermId>,
}


impl<TTerm> Index<TTerm>
    where TTerm: Hash + Ord
{
    fn index_document<TIter>(&mut self, iter: &mut TIter) -> DocId
        where TIter: Iterator<Item = TTerm>
    {
        DocId::none()
    }

    fn query_atom(&self, atom: &TTerm) -> Vec<DocId> {
        // if let Some(term_id) = self.vocabulary.get(atom) {
        //     if let Ok(index) = self.listings.binary_search_by_key(term_id, |&(t_id, _)| t_id) {
        //         let listing = self.listings[index].1;
        //         let posting_iter = PostingIterator::new(BlockIter::new(&self.page_manager,
        //                                                                listing.block_list,
        //                                                                listing.last_block_id));
        //     }
        // }
        vec![]
    }
}
