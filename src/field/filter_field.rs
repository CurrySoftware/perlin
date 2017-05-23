use std::ops::{Deref, DerefMut};
use std::hash::Hash;

use perlin_core::index::Index;
use perlin_core::index::vocabulary::TermId;


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

