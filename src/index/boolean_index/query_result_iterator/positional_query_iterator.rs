use utils::seeking_iterator::{PeekableSeekable, SeekingIterator};

use storage::Storage;

use storage::compression::VByteDecoder;

use index::boolean_index::DocumentTerms;
use index::boolean_index::boolean_query::PositionalOperator;
use index::boolean_index::posting::Posting;
use index::boolean_index::query_result_iterator::QueryResultIterator;

pub struct PositionalQueryIterator<'a> {
    operator: PositionalOperator,
    possible_documents: Box<PeekableSeekable<QueryResultIterator<'a>>>,
    pattern: Vec<(u32, u64)>,
    doc_store: &'a Storage<DocumentTerms>,
}

impl<'a> PositionalQueryIterator<'a> {
    pub fn new(operator: PositionalOperator,
               possible_documents: PeekableSeekable<QueryResultIterator<'a>>,
               pattern: Vec<(u32, u64)>,
               doc_store: &'a Storage<DocumentTerms>)
               -> Self {
        PositionalQueryIterator {
            operator: operator,
            possible_documents: Box::new(possible_documents),
            pattern: pattern,
            doc_store: doc_store,
        }
    }

    pub fn estimate_length(&self) -> usize {
        self.possible_documents.inner().estimate_length()
    }



    fn next_positional(&mut self) -> Option<Posting> {
        loop {
            let posting = try_option!(self.possible_documents.next());
            let enc_docterms = self.doc_store.get(*posting.doc_id()).unwrap();
            let docterms = VByteDecoder::new(enc_docterms.as_slice()).collect::<Vec<_>>();
            if match_pattern(docterms, &self.pattern) {
                return Some(posting);
            }
        }
    }
}

impl<'a> Iterator for PositionalQueryIterator<'a> {
    type Item = Posting;

    fn next(&mut self) -> Option<Posting> {
        match self.operator {
            PositionalOperator::InOrder => self.next_positional(),
        }
    }
}

impl<'a> SeekingIterator for PositionalQueryIterator<'a> {
    type Item = Posting;

    fn next_seek(&mut self, target: &Self::Item) -> Option<Self::Item> {
        self.possible_documents.peek_seek(target);
        self.next()
    }
}

// This is not optimal
// TODO: Make this optimal
fn match_pattern(data: Vec<usize>, pattern: &Vec<(u32, u64)>) -> bool {
    // Lets start searching for that pattern
    // Pointer to a document term
    let mut ptr = 0;
    // Pointer to the current position in the pattern
    let mut pos = 0;
    // Stores the ptr in case of match
    let mut tmp = 0;
    while ptr < data.len() {
        // If the term matches the pattern
        if data[ptr] == pattern[pos].1 as usize {
            if pos == 0 {
                tmp = ptr;
            }
            // Advance in pattern and in document
            let curr = pattern[pos].0;
            pos += 1;
            if pos == pattern.len() {
                return true;
            }
            // hmmm. does not look so cool...
            ptr = (ptr as isize + (pattern[pos].0 as isize - curr as isize) as isize) as usize;
        } else {
            // Else reset pattern and advance in document
            if pos > 0 {
                pos = 0;
                ptr = tmp;
            }
            ptr += 1;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::match_pattern;

    #[test]
    fn match_pattern_basic() {
        assert!(match_pattern(vec![1, 2, 3, 4, 5, 6, 7], &vec![(0, 4), (1, 5)]));
        assert!(match_pattern(vec![1, 2, 3, 4, 5, 6, 7], &vec![(0, 4), (2, 6)]));
    }

    #[test]
    fn match_pattern_rec() {
        assert!(match_pattern(vec![1, 2, 1, 2, 1, 4],
                              &vec![(0, 1), (1, 2), (2, 1), (3, 4)]));
    }

}
