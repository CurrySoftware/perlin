use std::hash::Hash;
use std::collections::HashMap;

use page_manager::{RamPageCache};
use index::listing::Listing;
use index::posting::{DocId, Posting};
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
    pub fn index_document<TIter>(&mut self, iter: &mut TIter) -> DocId
        where TIter: Iterator<Item = TTerm>
    {
        DocId::none()
    }

    pub fn query_atom(&self, atom: &TTerm) -> Vec<Posting> {
        if let Some(term_id) = self.vocabulary.get(atom) {
            if let Ok(index) = self.listings.binary_search_by_key(term_id, |&(t_id, _)| t_id) {
                return self.listings[index].1.posting_iter(&self.page_manager).collect::<Vec<_>>();
            }
        }
        vec![]
    }
}
