
use utils::byte_code::{ByteDecodable, ByteEncodable};
use utils::compression::*;

// For each term-document pair the doc_id and the
// positions of the term inside the document are stored
pub type Posting = (u64 /* doc_id */, Vec<u32> /* positions */);
pub type Listing = Vec<Posting>;

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

// TODO: Errorhandling
impl ByteDecodable for Vec<Posting> {
    fn decode<TIterator: Iterator<Item = u8>>(bytes: TIterator) -> Result<Self, String> {
        let mut decoder = VByteDecoder::new(bytes);
        let postings_len = decoder.next().unwrap();
        let mut postings = Vec::with_capacity(postings_len);
        for _ in 0..postings_len {
            let doc_id = decoder.next().unwrap();
            let positions_len = decoder.next().unwrap();
            let mut positions = Vec::with_capacity(positions_len as usize);
            let mut last_position = 0;
            for _ in 0..positions_len {
                last_position += decoder.next().unwrap();
                positions.push(last_position as u32);
            }
            postings.push((doc_id as u64, positions));
        }
        Ok(postings)
    }
}
