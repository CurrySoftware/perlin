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

impl ByteEncodable for String {
    fn encode(&self) -> Vec<u8> {
        let mut result = Vec::with_capacity(self.len());
        result.extend_from_slice(self.as_bytes());
        result
    }
}

impl ByteDecodable for String {
    fn decode(bytes: Vec<u8>) -> Self {
        String::from_utf8(bytes).unwrap()
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
    fn decode(bytes: Vec<u8>) -> Self {
        read_u64(bytes.as_slice()) as usize
    }
}

impl<TTerm: Ord + ByteDecodable + ByteEncodable> BooleanIndex<TTerm> {
    fn write_terms<TWrite: Write>(&self, write: &mut TWrite) -> std::io::Result<()> {
        for term in &self.index {
            let term_bytes = encode_term(&term);
            let term_bytes_len: [u8; 4] =
                unsafe { transmute::<_, [u8; 4]>(term_bytes.len() as u32) };
            write.write(&term_bytes_len).unwrap();
            write.write(term_bytes.as_slice()).unwrap();
        }
        write.flush().unwrap();
        Ok(())
    }

    fn read_terms<TRead: Read>(read: &mut TRead) -> BTreeMap<TTerm, Vec<Posting>> {
        let mut bytes = Vec::new();
        read.read_to_end(&mut bytes).unwrap();

        let mut ptr = 0;
        let mut result = BTreeMap::new();
        while ptr < bytes.len() {
            let entry_size = read_u32(&bytes[ptr..ptr + 4]) as usize;
            ptr += 4;
            let term_posting = decode_term(&bytes[ptr..ptr + entry_size]);
            result.insert(term_posting.0, term_posting.1);
            ptr += entry_size;
        }
        result
    }
}

fn decode_term<TTerm: ByteDecodable>(f: &[u8]) -> (TTerm, Vec<Posting>) {
    let term_len: u8 = f[0] + 1;
    let term_bytes_vec = Vec::from(&f[1..(term_len) as usize]);
    let term = TTerm::decode(term_bytes_vec);
    let mut ptr = term_len as usize;
    let mut postings = Vec::with_capacity(100);
    while ptr < f.len() {
        // 8bytes doc_id
        let doc_id = read_u64(&f[ptr..ptr + 8]);
        ptr += 8;
        let positions_len = read_u32(&f[ptr..ptr + 4]);
        ptr += 4;
        let positions = unsafe {
            std::slice::from_raw_parts(f[ptr..].as_ptr() as *const u32, positions_len as usize)
        };
        ptr += positions_len as usize * 4 as usize;
        let mut positions_vec = Vec::with_capacity(positions_len as usize);
        positions_vec.extend_from_slice(positions);
        postings.push((doc_id, positions_vec));
    }
    (term, postings)
}


// Writes the term to a file.
// Layout:
// [u8; 1] length of term in bytes
// [u8] term
// loop until all bytes read.
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
    fn write_to<TTarget: Write>(&self, target: &mut TTarget) -> std::io::Result<()> {
        self.write_terms(target)
    }

    fn read_from<TSource: Read>(source: &mut TSource) -> std::io::Result<Self> {
        let mut btree = BTreeMap::new();
        for term in Self::read_terms(source) {
            btree.insert(term.0, term.1);
        }
        Ok(BooleanIndex {
            document_count: 0,
            index: btree,
        })
    }
}


fn read_u16(barry: &[u8]) -> u16 {
    let mut array = [0u8; 2];
    for (&x, p) in barry.iter().zip(array.iter_mut()) {
        *p = x;
    }
    unsafe { transmute::<_, u16>(array) }
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



#[cfg(test)]
mod tests {
    use index::boolean_index::BooleanIndex;
    use index::boolean_index::tests::prepare_index;
    use index::PersistentIndex;
    use std::io::Cursor;
    
    #[test]
    fn basic_encoding_decoding() {
        let index = prepare_index();
        let mut bytes: Vec<u8> = vec![];
        index.write_to(&mut bytes).unwrap();
        let mut buff = Cursor::new(bytes.clone());
        let mut bytes_2: Vec<u8> = vec![];
        BooleanIndex::<usize>::read_from(&mut buff).unwrap().write_to(&mut bytes_2).unwrap();
        assert_eq!(bytes, bytes_2);        
    }
}
