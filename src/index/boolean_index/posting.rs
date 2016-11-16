use std::io::{Seek, SeekFrom};
use std::cmp::Ordering;

use utils::owning_iterator::SeekingIterator;
use storage::compression::VByteDecoder;
use chunked_storage::chunk_ref::ChunkRef;

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

pub struct PostingDecoder<'a> {
    decoder: VByteDecoder<ChunkRef<'a>>,
    last_doc_id: u64,
    len: usize,
}

impl<'a> PostingDecoder<'a> {
    pub fn new(chunk_ref: ChunkRef<'a>) -> Self {
        PostingDecoder {
            len: chunk_ref.get_total_postings(),
            decoder: VByteDecoder::new(chunk_ref),
            last_doc_id: 0,
        }
    }

    pub fn len(&self) -> usize {
        self.len
    }
}

impl<'a> Iterator for PostingDecoder<'a> {
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

impl<'a> SeekingIterator for PostingDecoder<'a> {
    type Item = Posting;

    //TODO: Rethink and explain this method
    fn next_seek(&mut self, other: &Self::Item) -> Option<Self::Item> {
        if self.last_doc_id >= *other.doc_id() {
            return self.next();
        }
        let (doc_id, offset) = self.decoder.underlying().doc_id_offset(other.doc_id());
        self.decoder.seek(SeekFrom::Start(offset as u64)).unwrap();
        let mut v = try_option!(self.next());
        v.0 = doc_id;
        self.last_doc_id = doc_id;        
        if v >= *other {
            return Some(v);
        }
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


#[cfg(test)]
mod tests{
    use super::*;
        
    use utils::persistence::Volatile;
    use utils::owning_iterator::SeekingIterator;
    use chunked_storage::ChunkedStorage;
    use storage::RamStorage;

    #[test]
    fn decoder() {
        let mut storage = ChunkedStorage::new(10, Box::new(RamStorage::new()));
        {
            let mut chunk = storage.new_chunk(0);
            let data = vec![0x81, 0x81, 0x80];
            for i in 1..1000 {
                chunk.write_posting(i, &data).unwrap();
            }
        }
        {
            let decoder = PostingDecoder::new(storage.get(0));
            assert_eq!(decoder.collect::<Vec<_>>(), (1..1000).map(|i| Posting::new(i, vec![0])).collect::<Vec<_>>());
        }
    }
    
    #[test]
    fn seek_decoding() {
        let mut storage = ChunkedStorage::new(10, Box::new(RamStorage::new()));
        {
            let mut chunk = storage.new_chunk(0);
            let data = vec![0x81, 0x81, 0x80];
            for i in 1..1000 {
                chunk.write_posting(i, &data).unwrap();
            }
        }
        {
            let mut decoder = PostingDecoder::new(storage.get(0));
            assert_eq!(decoder.next_seek(&Posting::new(5, vec![0])), Some(Posting::new(5, vec![0])));
            assert_eq!(decoder.next_seek(&Posting::new(10, vec![0])), Some(Posting::new(10, vec![0])));
            assert_eq!(decoder.next_seek(&Posting::new(100, vec![0])).unwrap(), Posting::new(100, vec![0]));
            assert_eq!(decoder.next().unwrap(), Posting::new(101, vec![0]));
            assert_eq!(decoder.next_seek(&Posting::new(200, vec![0])).unwrap(), Posting::new(200, vec![0]));
            assert_eq!(decoder.next().unwrap(), Posting::new(201, vec![0]));
            assert_eq!(decoder.next_seek(&Posting::new(800, vec![0])).unwrap(), Posting::new(800, vec![0]));
            assert_eq!(decoder.next().unwrap(), Posting::new(801, vec![0]));
            assert_eq!(decoder.next_seek(&Posting::new(997, vec![0])).unwrap(), Posting::new(997, vec![0]));
            assert_eq!(decoder.next().unwrap(), Posting::new(998, vec![0]));
            assert_eq!(decoder.next().unwrap(), Posting::new(999, vec![0]));
            assert_eq!(decoder.next(), None);
        }
        
    }

}
