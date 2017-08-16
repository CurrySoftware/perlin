use std::hash::Hash;

use perlin_core::index::Index;
use perlin_core::index::vocabulary::TermId;


pub struct Filter<T>(Vec<(usize, T, TermId)>);

impl<T: Hash + Eq + Ord + Clone + 'static> Filter<T> {
    pub fn commit(&mut self, index: &Index<T>) {
        let mut sorted_terms: Vec<(usize, T, TermId)> = index.iterate_terms()
            .map(|(t, term_id)| (index.term_df(term_id), t.clone(), *term_id))
            .collect::<Vec<_>>();

        sorted_terms.sort_by(|a, b| a.0.cmp(&b.0).reverse());
        self.0 = sorted_terms;

    }

    pub fn frequent_terms<'a>(&'a self) -> Box<Iterator<Item = (usize, &T, TermId)> + 'a> {
        Box::new(self.0.iter().map(move |&(ref df, ref t, ref term_id)| (*df, t, *term_id)))
    }

    pub fn new() -> Self {
        Filter(vec![])
    }
}
