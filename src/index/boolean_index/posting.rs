use std::io::Read;
use std::cmp::Ordering;

use chunked_storage::ChunkedStorage;
use utils::owning_iterator::SeekingIterator;
use storage::compression::{vbyte_encode, VByteDecoder};
use storage::{ByteDecodable, ByteEncodable, DecodeResult, DecodeError};

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

    pub fn positions(&self) -> &Positions {
        &self.1
    }
}

pub struct PostingDecoder<R: Read> {
    decoder: VByteDecoder<R>,
    last_doc_id: u64
}

impl<R: Read> PostingDecoder<R> {
    pub fn new(read: R) -> Self {
        PostingDecoder{
            decoder: VByteDecoder::new(read),
            last_doc_id: 0
        }
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

pub fn decode_from_storage(storage: &ChunkedStorage, id: u64) -> Option<Listing> {
    // Get hot listing
    let mut chunk = storage.get(id);
    let listing = decode_from_chunk_ref(&mut chunk).unwrap();
    Some(listing)
}


/// Returns the decoded Listing or the (`doc_id`, `position`) pair where an error occured
fn decode_from_chunk_ref<R: Read>(read: &mut R) -> Result<Listing, (usize, usize)> {
    let mut decoder = VByteDecoder::new(read);
    let mut postings = Vec::new();
    let mut base_doc_id = 0;
    while let Some(doc_id) = decoder.next() {
        base_doc_id += doc_id as u64;
        let positions_len = try!(decoder.next().ok_or((base_doc_id as usize, 0)));
        let mut positions = Vec::with_capacity(positions_len as usize);
        let mut last_position = 0;
        for i in 0..positions_len {
            last_position += try!(decoder.next().ok_or((base_doc_id as usize, i)));
            positions.push(last_position as u32);
        }
        postings.push(Posting::new(base_doc_id, positions));
    }
    Ok(postings)
}

impl ByteEncodable for Listing {
    fn encode(&self) -> Vec<u8> {
        let mut bytes: Vec<u8> = Vec::new();
        bytes.append(&mut vbyte_encode(self.len()));
        for posting in self {
            bytes.append(&mut vbyte_encode(*posting.doc_id() as usize));
            bytes.append(&mut vbyte_encode(posting.positions().len() as usize));
            let mut last_position = 0;
            for position in &posting.1 {
                bytes.append(&mut vbyte_encode((*position - last_position) as usize));
                last_position = *position;
            }
        }
        bytes
    }
}

impl ByteDecodable for Vec<Posting> {
    fn decode<R: Read>(read: &mut R) -> DecodeResult<Self> {
        let mut decoder = VByteDecoder::new(read);
        if let Some(postings_len) = decoder.next() {
            let mut postings = Vec::with_capacity(postings_len);
            for _ in 0..postings_len {
                let doc_id = try!(decoder.next().ok_or(DecodeError::MalformedInput));
                let positions_len = try!(decoder.next().ok_or(DecodeError::MalformedInput));
                let mut positions = Vec::with_capacity(positions_len as usize);
                let mut last_position = 0;
                for _ in 0..positions_len {
                    last_position += try!(decoder.next().ok_or(DecodeError::MalformedInput));
                    positions.push(last_position as u32);
                }
                postings.push(Posting::new(doc_id as u64, positions));
            }
            Ok(postings)
        } else {
            Err(DecodeError::MalformedInput)
        }
    }
}
