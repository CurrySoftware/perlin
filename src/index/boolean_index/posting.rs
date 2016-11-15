use std::io::Read;
use std::cmp::Ordering;

use utils::owning_iterator::SeekingIterator;
use storage::compression::VByteDecoder;

// For each term-document pair the doc_id and the
// positions of the term inside the document is stored
#[derive(Debug, Eq)]
pub struct Posting(pub DocId, pub Positions);

pub type DocId = u64;
pub type Positions = Vec<u32>;
pub type Listing = Vec<Posting>;

impl Posting {
    pub fn new(doc_id: DocId, positions: Positions) -> Self {
        Posting(doc_id, positions)
    }

    // TODO: Does it have an impact if we declare the
    // #[inline]-attribute on these kinds of functions?
    pub fn doc_id(&self) -> &DocId {
        &self.0
    }

    // TODO: Decode positions lazily
    pub fn positions(&self) -> &Positions {
        &self.1
    }
}

pub struct PostingDecoder<R: Read> {
    decoder: VByteDecoder<R>,
    last_doc_id: u64,
    len: usize,
}

impl<R: Read> PostingDecoder<R> {
    pub fn new(read: R, len: usize) -> Self {
        PostingDecoder {
            decoder: VByteDecoder::new(read),
            last_doc_id: 0,
            len: len,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }
}

impl<R: Read> Iterator for PostingDecoder<R> {
    type Item = Posting;

    fn next(&mut self) -> Option<Self::Item> {
        let delta_doc_id = try_option!(self.decoder.next()) as u64;
        let positions_len = try_option!(self.decoder.next());
        let mut positions = Vec::with_capacity(positions_len as usize);
        let mut last_position = 0;
        for _ in 0..positions_len {
            last_position += try_option!(self.decoder.next());
            positions.push(last_position as u32);
        }
        self.last_doc_id += delta_doc_id;
        Some(Posting::new(self.last_doc_id, positions))
    }
}

impl<R: Read> SeekingIterator for PostingDecoder<R> {
    type Item = Posting;

    fn next_seek(&mut self, other: &Self::Item) -> Option<Self::Item> {
        loop {
            let v = try_option!(self.next());
            if v >= *other {
                return Some(v);
            }
        }
    }
}


// When we compare postings, we usually only care about doc_ids.
// For comparisons that consider positions have a look at
// `index::boolean_index::query_result_iterator::nary_query_iterator::positional_intersect` ...
impl Ord for Posting {
    fn cmp(&self, other: &Self) -> Ordering {
        self.doc_id().cmp(other.doc_id())
    }
}

impl PartialEq for Posting {
    fn eq(&self, other: &Self) -> bool {
        self.doc_id().eq(other.doc_id())
    }
}

impl PartialOrd for Posting {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.doc_id().partial_cmp(other.doc_id())
    }
}
