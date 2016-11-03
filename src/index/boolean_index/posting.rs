use std::io::Read;
use chunked_storage::ChunkedStorage;
use storage::compression::{vbyte_encode, VByteDecoder};
use storage::{ByteDecodable, ByteEncodable, DecodeResult, DecodeError};

// For each term-document pair the doc_id and the
// positions of the term inside the document are stored
pub type Posting = (u64 /* doc_id */, Vec<u32> /* positions */);
pub type Listing = Vec<Posting>;

pub fn decode_from_storage(storage: &ChunkedStorage, id: u64) -> Option<Listing> {
    // Get hot listing
    let mut chunk = storage.get_current(id);
    let mut listing = decode_from_chunk_ref(&mut chunk).unwrap();
    Some(listing)
}

/// Returns the decoded Listing or the (`doc_id`, `position`) pair where an error occured
fn decode_from_chunk_ref<R: Read>(read: &mut R) -> Result<Listing, (usize, usize)> {
    let mut decoder = VByteDecoder::new(read.bytes());
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
        postings.push((base_doc_id, positions));
    }
    Ok(postings)
}

impl ByteEncodable for Listing {
    fn encode(&self) -> Vec<u8> {
        let mut bytes: Vec<u8> = Vec::new();
        bytes.append(&mut vbyte_encode(self.len()));
        for posting in self {
            bytes.append(&mut vbyte_encode(posting.0 as usize));
            bytes.append(&mut vbyte_encode(posting.1.len() as usize));
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
        let mut decoder = VByteDecoder::new(read.bytes());
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
                postings.push((doc_id as u64, positions));
            }
            Ok(postings)
        } else {
            Err(DecodeError::MalformedInput)
        }
    }
}
