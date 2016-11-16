use std::fmt;
use std::mem;
use std::io::Read;

use storage::{ByteEncodable, ByteDecodable, DecodeResult, DecodeError};
use storage::compression::{VByteDecoder, VByteEncoded};
use chunked_storage::SIZE;



// This struct is becoming pretty bloated.
// TODO: Check what is necessary.
pub struct HotIndexingChunk {
    last_doc_id: u64,
    total_postings: u64,
    // TODO: Can we remove the indirection that Vec implies?
    archived_chunks: Vec<(u64, u16, u32)>,
    first_doc_id: u64,
    pub capacity: u16,
    offset: u16,
    overflow: bool,
    pub data: [u8; SIZE],
}

pub struct IndexingChunk {
    capacity: u16, // 2
    data: [u8; SIZE], //
}

impl PartialEq for IndexingChunk {
    fn eq(&self, other: &IndexingChunk) -> bool {
        self.capacity == other.capacity && self.get_bytes() == other.get_bytes()
    }
}

impl Eq for IndexingChunk {}

impl PartialEq for HotIndexingChunk {
    fn eq(&self, other: &HotIndexingChunk) -> bool {
        self.capacity == other.capacity && self.last_doc_id == other.last_doc_id &&
        self.archived_chunks == other.archived_chunks && self.get_bytes() == other.get_bytes()
    }
}

impl fmt::Debug for IndexingChunk {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        try!(write!(f, "IndexingChunk has {} spare bytes!\n", self.capacity));
        try!(write!(f, "Data: {:?}", self.get_bytes()));
        Ok(())
    }
}

impl fmt::Debug for HotIndexingChunk {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        try!(write!(f,
                    "HotIndexingChunk has {} predecessors: {:?}\n And {} spare bytes! Its last document id is {}\n",
                    self.archived_chunks.len(),
                    self.archived_chunks,
                    self.capacity,
                    self.last_doc_id));
        try!(write!(f, "Data: {:?}", self.get_bytes()));
        Ok(())
    }
}

impl From<HotIndexingChunk> for IndexingChunk {
    fn from(chunk: HotIndexingChunk) -> Self {
        IndexingChunk {
            capacity: chunk.capacity,
            data: chunk.data,
        }
    }
}

impl IndexingChunk {
    pub fn get_bytes(&self) -> &[u8] {
        &self.data[0..SIZE - self.capacity as usize] as &[u8]
    }
}

impl Default for HotIndexingChunk {
    fn default() -> Self {
        Self::new()
    }
}


impl HotIndexingChunk {
    pub fn new() -> Self {
        HotIndexingChunk {
            last_doc_id: 0,
            capacity: SIZE as u16,
            archived_chunks: Vec::new(),
            total_postings: 0,
            first_doc_id: 0,
            offset: 0,
            overflow: true,
            data: unsafe { mem::uninitialized() },
        }
    }

    #[inline]
    pub fn get_last_doc_id(&self) -> u64 {
        self.last_doc_id
    }

    #[inline]
    pub fn get_total_postings(&self) -> usize {
        self.total_postings as usize
    }

    #[inline]
    pub fn add_doc_id(&mut self, doc_id: u64) {
        // Increment last doc id
        self.last_doc_id = doc_id;
        // Increment #postings
        self.total_postings += 1;
        // If last operation was an archive or new
        if self.overflow {
            // set the offset and the first doc id of this chunk
            // Used to be able to
            // 1. decode doc_ids faster
            // 2. be able to remove certain postings
            self.offset = SIZE as u16 - self.capacity;
            self.first_doc_id = doc_id;
            self.overflow = false;
        }
    }



    //TODO: Explain and rethink method
    pub fn doc_id_offset(&self, doc_id: &u64) -> (u64, usize) {
        if self.archived_chunks.is_empty() {
            return (self.first_doc_id, 0);
        }
        
        let mut index = match self.archived_chunks.binary_search_by_key(doc_id, |&(doc_id, _, _)| doc_id) {
            Ok(index) => index,
            Err(index) if index > 0 => index - 1,
            Err(index) => index,
        };

        
        let ref_doc_id = self.archived_chunks[index].0;
        // when chunks are overflowing, first_doc_id and offset are semantically wrong
        // Therefor we look for the first chunk that statisfis the condition
        // self.archived_chunks.where(|c| c.doc_id <= doc).map(|c| c.doc_id).max()
        while index > 0 {
            if self.archived_chunks[index - 1].0 < ref_doc_id {
                break;
            }
            index -= 1;
        }
        (ref_doc_id, index * SIZE + (self.archived_chunks[index].1 as usize))
    }

    pub fn archive(&mut self, at: u32) -> IndexingChunk {
        self.archived_chunks.push((self.first_doc_id, self.offset, at));
        let result = IndexingChunk {
            capacity: self.capacity,
            data: self.data,
        };
        self.overflow = true;
        self.capacity = SIZE as u16;
        result
    }

    pub fn get_bytes(&self) -> &[u8] {
        &self.data[0..SIZE - self.capacity as usize] as &[u8]
    }

    pub fn archived_chunks(&self) -> &Vec<(u64, u16, u32)> {
        &self.archived_chunks
    }
}

impl ByteDecodable for IndexingChunk {
    fn decode<R: Read>(read: &mut R) -> DecodeResult<Self> {
        let mut decoder = VByteDecoder::new(read);
        let mut result: IndexingChunk;
        {
            let capacity = try!(decoder.next().ok_or(DecodeError::MalformedInput));
            result = IndexingChunk {
                capacity: capacity as u16,
                data: unsafe { mem::uninitialized() },
            };
        }
        try!(decoder.read_exact(&mut result.data));
        Ok(result)
    }
}

impl ByteEncodable for IndexingChunk {
    fn encode(&self) -> Vec<u8> {
        let mut result = Vec::with_capacity(SIZE + 24);
        {
            let mut write_ptr = &mut result;
            VByteEncoded::new(self.capacity as usize).write_to(write_ptr).unwrap();
        }
        result.extend_from_slice(&self.data);
        result
    }
}

impl ByteDecodable for HotIndexingChunk {
    fn decode<R: Read>(read: &mut R) -> DecodeResult<Self> {
        let mut decoder = VByteDecoder::new(read);
        let mut result: HotIndexingChunk;
        {
            let capacity = try!(decoder.next().ok_or(DecodeError::MalformedInput));
            let last_doc_id = try!(decoder.next().ok_or(DecodeError::MalformedInput));
            let total_postings = try!(decoder.next().ok_or(DecodeError::MalformedInput));
            let first_doc_id = try!(decoder.next().ok_or(DecodeError::MalformedInput));
            let offset = try!(decoder.next().ok_or(DecodeError::MalformedInput));
            let overflow = try!(decoder.next().ok_or(DecodeError::MalformedInput));
            let archived_chunks_len = try!(decoder.next().ok_or(DecodeError::MalformedInput));
            let mut archived_chunks = Vec::with_capacity(archived_chunks_len);
            for _ in 0..archived_chunks_len {
                let doc_id = try!(decoder.next().ok_or(DecodeError::MalformedInput)) as u64;
                let offset = try!(decoder.next().ok_or(DecodeError::MalformedInput)) as u16;
                let chunk_id = decoder.next().ok_or(DecodeError::MalformedInput)? as u32;
                archived_chunks.push((doc_id, offset, chunk_id));
            }
            result = HotIndexingChunk {
                capacity: capacity as u16,
                last_doc_id: last_doc_id as u64,
                total_postings: total_postings as u64,
                archived_chunks: archived_chunks,
                first_doc_id: first_doc_id as u64,
                offset: offset as u16,
                overflow: overflow == 0,
                data: unsafe { mem::uninitialized() },
            };
        }
        try!(decoder.read_exact(&mut result.data));
        Ok(result)
    }
}

impl ByteEncodable for HotIndexingChunk {
    fn encode(&self) -> Vec<u8> {
        let mut result = Vec::with_capacity(SIZE + 24);
        {
            let mut write_ptr = &mut result;
            VByteEncoded::new(self.capacity as usize).write_to(write_ptr).unwrap();
            VByteEncoded::new(self.last_doc_id as usize).write_to(write_ptr).unwrap();
            VByteEncoded::new(self.total_postings as usize).write_to(write_ptr).unwrap();
            VByteEncoded::new(self.first_doc_id as usize).write_to(write_ptr).unwrap();
            VByteEncoded::new(self.offset as usize).write_to(write_ptr).unwrap();
            VByteEncoded::new(self.overflow as usize).write_to(write_ptr).unwrap();
            VByteEncoded::new(self.archived_chunks.len()).write_to(write_ptr).unwrap();
            for &(doc_id, offset, chunk_id) in &self.archived_chunks {
                VByteEncoded::new(doc_id as usize).write_to(write_ptr).unwrap();
                VByteEncoded::new(offset as usize).write_to(write_ptr).unwrap();
                VByteEncoded::new(chunk_id as usize).write_to(write_ptr).unwrap();
            }
        }
        result.extend_from_slice(&self.data);
        result
    }
}

#[cfg(test)]
mod tests {

    mod hot_indexing_chunk {
        use super::super::*;
        use storage::{ByteDecodable, ByteEncodable};
        use chunked_storage::SIZE;

        #[test]
        fn encoding_basic() {
            let chunk = HotIndexingChunk::new();
            let bytes = chunk.encode();
            assert_eq!(chunk,
                       HotIndexingChunk::decode(&mut (&bytes as &[u8])).unwrap());
            assert_eq!(HotIndexingChunk::new(),
                       HotIndexingChunk::decode(&mut (&bytes as &[u8])).unwrap());
        }

        #[test]
        fn encoding_with_data() {
            let mut chunk = HotIndexingChunk::new();
            chunk.data = [1; SIZE];
            chunk.capacity = 0;
            let bytes = chunk.encode();
            assert_eq!(chunk,
                       HotIndexingChunk::decode(&mut (&bytes as &[u8])).unwrap());
            assert_eq!(HotIndexingChunk::decode(&mut (&bytes as &[u8])).unwrap().get_bytes(),
                       &[1u8; SIZE] as &[u8]);
        }

        #[test]
        fn decoding_zeroed_data() {
            let bytes = vec![0u8; 100];
            assert!(HotIndexingChunk::decode(&mut (&bytes as &[u8])).is_err());
        }

        #[test]
        fn decoding_from_corrup_data() {
            let chunk = HotIndexingChunk::new();
            let bytes = chunk.encode();
            assert!(HotIndexingChunk::decode(&mut (&bytes[5..] as &[u8])).is_err());
            assert!(HotIndexingChunk::decode(&mut (&bytes[..100] as &[u8])).is_err());
            assert!(HotIndexingChunk::decode(&mut (&bytes.iter().map(|i| i % 2 * 128).collect::<Vec<_>>() as &[u8]))
                .is_err());
        }

        #[test]
        fn archive() {
            let mut chunk = HotIndexingChunk::new();
            chunk.data = [1; SIZE];
            chunk.capacity = 0;
            let archived = chunk.archive(0);
            assert_eq!(chunk.capacity, SIZE as u16);
            assert_eq!(chunk.get_bytes(), &[0u8; 0] as &[u8]);
            assert_eq!(archived.get_bytes(), &[1u8; SIZE] as &[u8]);
        }

        #[test]
        fn last_doc_id() {
            let mut chunk = HotIndexingChunk::new();
            assert_eq!(chunk.get_last_doc_id(), 0);
            chunk.add_doc_id(100);
            assert_eq!(chunk.get_last_doc_id(), 100);
        }

        #[test]
        fn add_doc_id_basic() {
            let mut chunk = HotIndexingChunk::new();
            assert_eq!(chunk.get_last_doc_id(), 0);
            assert_eq!(chunk.first_doc_id, 0);
            assert_eq!(chunk.offset, 0);
            assert_eq!(chunk.overflow, true);
            chunk.add_doc_id(100);
            assert_eq!(chunk.get_last_doc_id(), 100);
            assert_eq!(chunk.first_doc_id, 100);
            assert_eq!(chunk.offset, 0);
            assert_eq!(chunk.overflow, false);
            chunk.add_doc_id(101);
            assert_eq!(chunk.get_last_doc_id(), 101);
            assert_eq!(chunk.first_doc_id, 100);
            assert_eq!(chunk.offset, 0);
            assert_eq!(chunk.overflow, false);
        }

        #[test]
        fn add_doc_id_extended() {
            let mut chunk = HotIndexingChunk::new();
            assert_eq!(chunk.get_last_doc_id(), 0);
            assert_eq!(chunk.first_doc_id, 0);
            assert_eq!(chunk.offset, 0);
            assert_eq!(chunk.overflow, true);
            chunk.add_doc_id(100);
            assert_eq!(chunk.get_last_doc_id(), 100);
            assert_eq!(chunk.first_doc_id, 100);
            assert_eq!(chunk.offset, 0);
            assert_eq!(chunk.overflow, false);
            chunk.overflow = true;
            chunk.add_doc_id(101);
            assert_eq!(chunk.get_last_doc_id(), 101);
            assert_eq!(chunk.first_doc_id, 101);
            assert_eq!(chunk.offset, 0);
            assert_eq!(chunk.overflow, false);
        }

        #[test]
        fn doc_id_offset() {
            let mut chunk = HotIndexingChunk::new();
            chunk.archived_chunks = vec![(0, 0, 1), (24, 3, 2), (56, 15, 3), (77, 8, 13)];
            assert_eq!(chunk.doc_id_offset(&0), (0, 0));
            assert_eq!(chunk.doc_id_offset(&1), (0, 0));
            assert_eq!(chunk.doc_id_offset(&23), (0, 0));
            assert_eq!(chunk.doc_id_offset(&24), (24, SIZE + 3));
            assert_eq!(chunk.doc_id_offset(&25), (24, SIZE + 3));
            assert_eq!(chunk.doc_id_offset(&55), (24, SIZE + 3));
            assert_eq!(chunk.doc_id_offset(&56), (56, SIZE * 2 + 15));
            assert_eq!(chunk.doc_id_offset(&77), (77, SIZE * 3 + 8));
            assert_eq!(chunk.doc_id_offset(&78), (77, SIZE * 3 + 8));
        }

        #[test]
        fn doc_id_offset_overflow() {
            let mut chunk = HotIndexingChunk::new();
            chunk.archived_chunks = vec![(10, 0, 1), (124, 3, 2), (156, 15, 3), (177, 8, 13)];
            assert_eq!(chunk.doc_id_offset(&0), (10, 0));
        }
    }

    mod indexing_chunk {
        use super::super::*;
        use storage::{ByteDecodable, ByteEncodable};
        use chunked_storage::SIZE;

        #[test]
        fn encoding_basic() {
            let chunk = IndexingChunk {
                capacity: SIZE as u16,
                data: [0; SIZE],
            };
            let bytes = chunk.encode();
            assert_eq!(chunk.get_bytes(), &[0u8; 0] as &[u8]);
            assert_eq!(chunk,
                       IndexingChunk::decode(&mut (&bytes as &[u8])).unwrap());
            assert_eq!(IndexingChunk {
                           capacity: SIZE as u16,
                           data: [0; SIZE],
                       },
                       IndexingChunk::decode(&mut (&bytes as &[u8])).unwrap());
            assert_eq!(IndexingChunk::decode(&mut (&bytes as &[u8])).unwrap().get_bytes(),
                       &[0u8; 0] as &[u8]);
        }

        #[test]
        fn encoding_with_data() {
            let chunk = IndexingChunk {
                capacity: 0,
                data: [1; SIZE],
            };
            let bytes = chunk.encode();
            assert_eq!(chunk,
                       IndexingChunk::decode(&mut (&bytes as &[u8])).unwrap());
            assert_eq!(IndexingChunk {
                           capacity: 0,
                           data: [1; SIZE],
                       },
                       IndexingChunk::decode(&mut (&bytes as &[u8])).unwrap());
            assert_eq!(IndexingChunk::decode(&mut (&bytes as &[u8])).unwrap().get_bytes(),
                       &[1u8; SIZE] as &[u8]);
        }
    }


}
