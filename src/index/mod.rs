use std::collections::HashMap;

use page_manager::RamPageCache;
use index::listing::Listing;
use index::posting::DocId;
use index::vocabulary::{Vocabulary, TermId};

pub mod vocabulary;
pub mod posting;
mod listing;


pub struct Index<TTerm> {
    page_manager: RamPageCache,
    listings: Vec<(TermId, Listing)>,
    vocabulary: HashMap<TTerm, TermId>
}


impl<TTerm> Index<TTerm> {

    fn index_document<TIter>(&mut self, iter: &mut TIter) -> DocId
        where TIter: Iterator<Item=TTerm> {
        
    }

    fn query_atom(&self, atom: &TTerm) -> Vec<DocId> {
        if let Some(term_id) = self.vocabulary.get(atom) {
            
        } else {
            vec![]
        }
    }
    
}
