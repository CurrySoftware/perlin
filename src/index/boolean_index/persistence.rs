//! Implements persistence for `BooleanIndex`.
//! e.g. writing the index to a bytestream; reading the index from a bytestream.
//! The API-Entrypoints are defined in the trait `index::PersistentIndex`

use index::{PersistentIndex, ByteEncodable, ByteDecodable};
use index::boolean_index::BooleanIndex;
use index::boolean_index::posting::Posting;

use std::mem::transmute;
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
    fn decode(bytes: Vec<u8>) -> Result<Self, String> {
        String::from_utf8(bytes).map_err(|e| format!("{:?}", e))
    }
}

impl ByteEncodable for usize {
    fn encode(&self) -> Vec<u8> {
        let mut result = Vec::with_capacity(8);
        result.extend_from_slice(unsafe { &transmute::<_, [u8; 8]>(*self as u64) });
        result
    }
}

impl ByteDecodable for usize {
    fn decode(bytes: Vec<u8>) -> Result<Self, String> {
        Ok(read_u64(bytes.as_slice()) as usize)
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
        for term in &self.index {
            let term_bytes = encode_term(&term);
            let term_bytes_len: [u8; 4] =
                unsafe { transmute::<_, [u8; 4]>(term_bytes.len() as u32) };

            bytes.extend_from_slice(&term_bytes_len);
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

        let mut ptr = 0;
        let mut result = BTreeMap::new();
        while ptr < bytes.len() {
            let entry_size = read_u32(&bytes[ptr..ptr + 4]) as usize;
            ptr += 4;
            match decode_term(&bytes[ptr..ptr + entry_size]) {
                Ok(term_posting) => {
                    result.insert(term_posting.0, term_posting.1);
                    ptr += entry_size;
                }
                Err(e) => return Err(e),
            }
        }
        Ok(result)
    }
}

fn decode_term<TTerm: ByteDecodable>(f: &[u8]) -> Result<(TTerm, Vec<Posting>), String> {
    let term_len: u8 = f[0] + 1;
    let term_bytes_vec = Vec::from(&f[1..(term_len) as usize]);
    match TTerm::decode(term_bytes_vec) {
        Ok(term) => {
            let mut ptr = term_len as usize;
            let mut postings = Vec::with_capacity(100);
            while ptr < f.len() {
                // 8bytes doc_id
                let doc_id = read_u64(&f[ptr..ptr + 8]);
                ptr += 8;
                let positions_len = read_u32(&f[ptr..ptr + 4]);
                ptr += 4;
                let positions = unsafe {
                    std::slice::from_raw_parts(f[ptr..].as_ptr() as *const u32,
                                               positions_len as usize)
                };
                ptr += positions_len as usize * 4 as usize;
                let mut positions_vec = Vec::with_capacity(positions_len as usize);
                positions_vec.extend_from_slice(positions);
                postings.push((doc_id, positions_vec));
            }
            Ok((term, postings))
        }
        Err(e) => Err(e),
    }
}

// Represents a term-entry in the inverted index as a vector of bytes
// Layout:
// [u8; 1] length of term in bytes
// [u8] term0
// [u8; 8] doc_id
// [u8; 4] #positions
// [u8] u32 encoded positions
fn encode_term<TTerm: ByteEncodable>(term: &(&TTerm, &Vec<Posting>)) -> Vec<u8> {
    let mut bytes: Vec<u8> = Vec::with_capacity(10);
    let term_bytes = term.0.encode();
    let term_len: u8 = term_bytes.len() as u8;
    bytes.push(term_len);
    bytes.extend_from_slice(term_bytes.as_slice());
    for posting in term.1.iter() {
        let doc_id_bytes = unsafe { transmute::<_, [u8; 8]>(posting.0) };
        let positions_len_bytes: [u8; 4] =
            unsafe { transmute::<_, [u8; 4]>(posting.1.len() as u32) };
        let position_bytes = unsafe {
            std::slice::from_raw_parts(posting.1.as_ptr() as *const u8, posting.1.len() * 4)
        };
        bytes.extend_from_slice(&doc_id_bytes);
        bytes.extend_from_slice(&positions_len_bytes);
        bytes.extend_from_slice(position_bytes);
    }
    bytes
}

impl<TTerm: ByteDecodable + ByteEncodable + Ord> PersistentIndex for BooleanIndex<TTerm> {
    fn write_to<TTarget: Write>(&self, target: &mut TTarget) -> std::io::Result<usize> {
        self.write_terms(target)
    }

    fn read_from<TSource: Read>(source: &mut TSource) -> Result<Self, String> {
        Self::read_terms(source).map(|btree| {
            BooleanIndex {
                document_count: 0,
                index: btree,
            }
        })
    }
}

fn read_u32(barry: &[u8]) -> u32 {
    let mut array = [0u8; 4];
    for (&x, p) in barry.iter().zip(array.iter_mut()) {
        *p = x;
    }
    unsafe { transmute::<_, u32>(array) }
}

fn read_u64(barry: &[u8]) -> u64 {
    let mut array = [0u8; 8];
    for (&x, p) in barry.iter().zip(array.iter_mut()) {
        *p = x;
    }
    unsafe { transmute::<_, u64>(array) }
}

fn vbyte_encode(mut number: usize) -> Vec<u8>{
    let mut result = Vec::new();
    loop {
        result.insert(0, (number % 128) as u8);
        if number < 128 {
            break;
        } else {
            number /= 128;
        }
    }
    let len = result.len();
    result[len - 1] += 128;
    result
}

macro_rules! unwrap_or_return_none{
    ($operand:expr) => {
        if let Some(x) = $operand {
            x
        } else {
            return None;
        }
    }
}


struct VByteDecoder<T: Iterator<Item=u8>> {
    bytes: T
}

impl<T: Iterator<Item=u8>> VByteDecoder<T> {
    fn new(bytes: T) -> Self {
        VByteDecoder{
            bytes: bytes            
        }        
    }
}

impl<T: Iterator<Item=u8>> Iterator for VByteDecoder<T>{
    type Item = usize;
    fn next(&mut self) -> Option<Self::Item> {

        let mut result: usize = 0;
        loop {
            result *= 128;
            let val = unwrap_or_return_none!(self.bytes.next());            
            result += val as usize;
            if val >= 128 {
                result -= 128;
                break;
            }
        }
        Some(result)
    }
}


#[test]
fn test_vbyte_encode() {
    assert_eq!(vbyte_encode(0), vec![0x80]);
    assert_eq!(vbyte_encode(5), vec![0x85]);
    assert_eq!(vbyte_encode(127), vec![0xFF]);
    assert_eq!(vbyte_encode(128), vec![0x01, 0x80]);
    assert_eq!(vbyte_encode(130), vec![0x01, 0x82]);
    assert_eq!(vbyte_encode(255), vec![0x01, 0xFF]);
    assert_eq!(vbyte_encode(20_000), vec![0x01, 0x1C, 0xA0]);
    assert_eq!(vbyte_encode(0xFFFF), vec![0x03, 0x7F, 0xFF]);
}

#[test]
fn test_vbyte_decode() {
    assert_eq!(VByteDecoder::new(vec![0x80].into_iter()).collect::<Vec<_>>(), vec![0]);
    assert_eq!(VByteDecoder::new(vec![0x85].into_iter()).collect::<Vec<_>>(), vec![5]);
    assert_eq!(VByteDecoder::new(vec![0xFF].into_iter()).collect::<Vec<_>>(), vec![127]);
    assert_eq!(VByteDecoder::new(vec![0x80, 0x81].into_iter()).collect::<Vec<_>>(), vec![0, 1]);
    assert_eq!(VByteDecoder::new(vec![0x03, 0x7F, 0xFF, 0x01, 0x82, 0x85].into_iter()).collect::<Vec<_>>(), vec![0xFFFF, 130, 5]);
    assert_eq!(VByteDecoder::new(vec![0x80].into_iter()).collect::<Vec<_>>(), vec![0]);
}




#[cfg(test)]
mod tests {
    use index::boolean_index::BooleanIndex;
    use index::boolean_index::tests::prepare_index;
    use index::{Index, PersistentIndex};
    use std::io::Cursor;

    #[test]
    fn basic() {
        let index = prepare_index();
        let mut bytes: Vec<u8> = vec![];
        index.write_to(&mut bytes).unwrap();
        let mut buff = Cursor::new(bytes.clone());
        let mut bytes_2: Vec<u8> = vec![];
        BooleanIndex::<usize>::read_from(&mut buff).unwrap().write_to(&mut bytes_2).unwrap();
        assert_eq!(bytes, bytes_2);
    }

    #[test]
    fn length() {
        let index = prepare_index();
        let mut bytes: Vec<u8> = vec![];
        index.write_to(&mut bytes).unwrap();
        let mut buff = Cursor::new(bytes.clone());
        let mut bytes_2: Vec<u8> = vec![];
        let mut read_index = BooleanIndex::<usize>::read_from(&mut buff).unwrap();
        read_index.index_document(1..24);
        read_index.write_to(&mut bytes_2).unwrap();
        assert!(bytes.len() < bytes_2.len());
    }
}
