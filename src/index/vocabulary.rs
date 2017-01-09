use std::sync::{Arc, RwLock};

use std::hash::Hash;
use std::collections::HashMap;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Copy, Clone)]
pub struct TermId(pub u64);

#[derive(Clone)]
pub struct SharedVocabulary<TTerm>(Arc<RwLock<HashMap<TTerm, TermId>>>);

impl<TTerm: Hash + Eq> SharedVocabulary<TTerm> {
    pub fn new() -> Self {
        SharedVocabulary(Arc::new(RwLock::new(HashMap::new())))
    }
}

pub trait Vocabulary<TTerm> {
    fn get_or_add(&mut self, TTerm) -> TermId;
    fn get(&self, &TTerm) -> Option<TermId>;
}

impl<TTerm: Hash + Eq> Vocabulary<TTerm> for SharedVocabulary<TTerm>{
    fn get_or_add(&mut self, term: TTerm) -> TermId {
        {//Scope of read lock
            let read = self.0.read().unwrap();
            if let Some(term_id) = read.get(&term) {
                return *term_id;
            }            
        }
        {//Scope of write lock
            let mut write = self.0.write().unwrap();
            //between last time checking and write locking, the term could have already been added!
            if let Some(term_id) = write.get(&term) {
                return *term_id;
            }
            //It was obivously not added. so we will do this now!
            let term_id = TermId(write.len() as u64);
            write.insert(term, term_id);
            return term_id;
        }
    }

    fn get(&self, term: &TTerm) -> Option<TermId> {
        self.0.read().unwrap().get(term).map(|t| *t)
    }
}

impl<TTerm> Vocabulary<TTerm> for HashMap<TTerm, TermId> where TTerm: Hash + Eq{
    fn get_or_add(&mut self, term: TTerm) -> TermId {
        if !self.contains_key(&term) {
            let t_id = TermId(self.len() as u64);
            self.insert(term, t_id);
            return t_id;
        }
        *self.get(&term).unwrap()
    }

    #[inline]
    fn get(&self, term: &TTerm) -> Option<TermId> {
        self.get(term).map(|t| *t)
    }
}
