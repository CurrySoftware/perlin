//! Implements persistence for `BooleanIndex`.
//! e.g. writing the index to a bytestream; reading the index from a bytestream.
//! The API-Entrypoints are defined in the trait `index::PersistentIndex`


use index::PersistentIndex;
use index::storage::ram_storage::RamStorage;
use index::boolean_index::BooleanIndex;
use index::boolean_index::posting::Posting;
use utils::compression::{vbyte_encode, VByteDecoder};
use utils::byte_code::{ByteEncodable, ByteDecodable};

use std::io::{Read, Write};
use std::collections::BTreeMap;
use std;

const CHUNKSIZE: usize = 1_000_000;

impl ByteEncodable for String {
    fn encode(&self) -> Vec<u8> {
        let mut result = Vec::with_capacity(self.len());
        result.extend_from_slice(self.as_bytes());
        result
    }
}

impl ByteDecodable for String {
    fn decode<TIterator: Iterator<Item=u8>>(bytes: TIterator) -> Result<Self, String> {
        String::from_utf8(bytes.collect()).map_err(|e| format!("{:?}", e))
    }
}

impl ByteEncodable for usize {
    fn encode(&self) -> Vec<u8> {
        vbyte_encode(*self)
    }
}

impl ByteDecodable for usize {
    fn decode<TIterator: Iterator<Item=u8>>(bytes: TIterator) -> Result<Self, String> {
        let mut decoder = VByteDecoder::new(bytes.into_iter());
        if let Some(res) = decoder.next() {
            Ok(res)
        } else {
            Err("Tried to decode bytevector with variable byte code. Failed".to_string())
        }
    }
}


impl<TTerm: Ord + ByteDecodable + ByteEncodable> BooleanIndex<TTerm> {
    /// Writes all the terms with postings of the index to specified target
    /// Layout:
    /// [u8; 4] -> Number of bytes term + postings need encoded
    /// [u8] -> term + postings
    fn write_terms<TTarget: Write>(&self, target: &mut TTarget) -> std::io::Result<usize> {
        // Write blocks of 1MB to target
        let mut bytes = Vec::with_capacity(2 * CHUNKSIZE);
        for term in &self.term_ids {
            let term_postings = self.postings.get(*term.1).unwrap();
            let term_bytes = encode_term(&(term.0, &term_postings));
            bytes.extend_from_slice(term_bytes.as_slice());
            if bytes.len() > CHUNKSIZE {
                if let Err(e) = target.write(bytes.as_slice()) {
                    return Err(e);
                } else {
                    bytes.clear();
                }
            }
        }
        target.write(bytes.as_slice())
    }

    fn read_terms<TSource: Read>(source: &mut TSource)
                                 -> Result<BTreeMap<TTerm, Vec<Posting>>, String> {
        let mut bytes = Vec::new();
        if let Err(e) = source.read_to_end(&mut bytes) {
            return Err(format!("{:?}", e));
        }
        let mut decoder = VByteDecoder::new(bytes.into_iter());
        let mut result = BTreeMap::new();
        loop {
            match decode_term(&mut decoder) {
                Ok(Some(term_posting)) => {
                    result.insert(term_posting.0, term_posting.1);
                }
                Ok(None) => break,
                Err(e) => return Err(e),
            }
        }
        Ok(result)
    }
}

fn decode_term<TTerm: ByteDecodable>
    (decoder: &mut VByteDecoder)
     -> Result<Option<(TTerm, Vec<Posting>)>, String> {
    if let Some(term_len) = decoder.next() {
        match TTerm::decode(decoder.underlying_iterator().take(term_len as usize)) {
            Ok(term) => {
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
                Ok(Some((term, postings)))
            }
            Err(e) => Err(e),
        }
    } else {
        Ok(None)
    }

}

// Represents a term-entry in the inverted index as a vector of bytes
// Layout:
// [u8; 1] length of term in bytes
// [u8] term
// For every posting:
// [u8] stream of vbyte encoded numbers: doc_id, #positions, [positions]
fn encode_term<TTerm: ByteEncodable>(term: &(&TTerm, &Vec<Posting>)) -> Vec<u8> {
    let mut bytes: Vec<u8> = Vec::new();
    let term_bytes = term.0.encode();
    let mut term_len = vbyte_encode(term_bytes.len());
    bytes.append(&mut term_len);
    bytes.extend_from_slice(term_bytes.as_slice());
    bytes.append(&mut vbyte_encode(term.1.len()));
    for posting in term.1.iter() {
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


impl<TTerm: ByteDecodable + ByteEncodable + Ord> PersistentIndex for BooleanIndex<TTerm> {
    fn write_to<TTarget: Write>(&mut self, target: &mut TTarget) -> std::io::Result<usize> {
        self.write_terms(target)
    }

    fn read_from<TSource: Read>(source: &mut TSource) -> Result<Self, String> {
        let inv_index = Self::read_terms(source).unwrap();
        let mut index = BooleanIndex::new(Box::new(RamStorage::new()));
        for (term, listing) in inv_index {
            let term_id = index.term_ids.len() as u64;
            index.term_ids.insert(term, term_id);
            index.postings.store(term_id, listing).unwrap();
        }
        Ok(index)
    }
}

#[cfg(test)]
mod tests {
    use index::storage::ram_storage::RamStorage;
    use index::boolean_index::BooleanIndex;
    use index::boolean_index::tests::prepare_index;
    use index::{Index, PersistentIndex};
    use std::io::Cursor;

    #[test]
    fn simple() {
        let mut index = BooleanIndex::new(Box::new(RamStorage::new()));
        index.index_documents(vec![0..2]);
        let mut bytes: Vec<u8> = vec![];
        index.write_to(&mut bytes).unwrap();
        assert_eq!(bytes,
                   vec![129 /* #TermBytes */, 128 /* Term: 0 */, 129 /* #docs */,
                        128 /* doc_id */, 129 /* #positions */, 128 /* position: 0 */,
                        129 /* #TermBytes */, 129 /* Term: 1 */, 129 /* #docs */,
                        128 /* doc_id */, 129 /* #positions */, 129 /* position */]);
    }

    #[test]
    fn basic() {
        let mut index = prepare_index();
        let mut bytes: Vec<u8> = vec![];
        index.write_to(&mut bytes).unwrap();
        let mut buff = Cursor::new(bytes.clone());
        let mut bytes_2: Vec<u8> = vec![];
        BooleanIndex::<usize>::read_from(&mut buff).unwrap().write_to(&mut bytes_2).unwrap();
        assert_eq!(bytes, bytes_2);
    }
}
