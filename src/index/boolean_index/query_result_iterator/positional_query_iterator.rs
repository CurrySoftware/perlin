use utils::seeking_iterator::{PeekableSeekable, SeekingIterator};

use storage::Storage;

use index::boolean_index::DocumentTerms;
use index::boolean_index::posting::Posting;
use index::boolean_index::query_result_iterator::QueryResultIterator;

pub struct PositionalQueryIterator<'a> {
    possible_documents: Box<PeekableSeekable<QueryResultIterator<'a>>>,
    pattern: Vec<(u32, u64)>,
    doc_store: &'a Storage<DocumentTerms>
}

impl<'a> PositionalQueryIterator<'a> {
    pub fn new(possible_documents: PeekableSeekable<QueryResultIterator<'a>>, pattern: Vec<(u32, u64)>, doc_store: &'a Storage<DocumentTerms>) -> Self {
        PositionalQueryIterator {
            possible_documents: Box::new(possible_documents),
            pattern: pattern,
            doc_store: doc_store
        }
    }

    pub fn estimate_length(&self) -> usize {
        self.possible_documents.inner().estimate_length()
    }
    
    fn next_positional(&mut self) -> Option<Posting> {
        None
    }
}

impl<'a> Iterator for PositionalQueryIterator<'a> {
    type Item = Posting;

    fn next(&mut self) -> Option<Posting> {
        self.next_positional()
    }
}

impl<'a> SeekingIterator for PositionalQueryIterator<'a> {
    type Item = Posting;

    fn next_seek(&mut self, target: &Self::Item) -> Option<Self::Item> {
        self.possible_documents.peek_seek(target);
        self.next()
    }
}


