use std::fmt;
use std::mem;
use std::io::Read;

use storage::{ByteEncodable, ByteDecodable, DecodeResult, DecodeError};
use storage::compression::{VByteDecoder, VByteEncoded};
use chunked_storage::SIZE;



pub struct HotIndexingChunk {
    pub capacity: u16,
    last_doc_id: u64,
    // TODO: Can we remove that indirection?
    archived_chunks: Vec<u32>,
    pub data: [u8; SIZE],
}

pub struct IndexingChunk {
    capacity: u16, // 2
    data: [u8; SIZE], //
}

impl PartialEq for IndexingChunk {
    fn eq(&self, other: &IndexingChunk) -> bool {
        self.capacity == other.capacity && self.data.as_ref() == other.data.as_ref()
    }
}

impl Eq for IndexingChunk {}

impl PartialEq for HotIndexingChunk {
    fn eq(&self, other: &HotIndexingChunk) -> bool {
        self.capacity == other.capacity && self.last_doc_id == other.last_doc_id &&
        self.archived_chunks == other.archived_chunks && self.data.as_ref() == other.data.as_ref()
    }
}

impl fmt::Debug for IndexingChunk {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        try!(write!(f, "IndexingChunk has {} spare bytes!\n", self.capacity));
        try!(write!(f, "Data: {:?}", self.get_bytes()));
        Ok(())
    }
}

// TODO: Complete!
impl fmt::Debug for HotIndexingChunk {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        try!(write!(f,
                    "HotIndexingChunk has {} predecessors: {:?}\n And {} spare bytes!\n",
                    self.archived_chunks().len(),
                    self.archived_chunks(),
                    self.capacity));
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
            data: unsafe { mem::uninitialized() },
        }
    }

    #[inline]
    pub fn get_last_doc_id(&self) -> u64 {
        self.last_doc_id
    }

    pub fn set_last_doc_id(&mut self, new_id: u64) {
        self.last_doc_id = new_id;
    }

    pub fn archive(&mut self, at: u32) -> IndexingChunk {
        self.archived_chunks.push(at);
        let result = IndexingChunk {
            capacity: self.capacity,
            data: self.data,
        };
        self.capacity = SIZE as u16;
        result
    }

    pub fn get_bytes(&self) -> &[u8] {
        &self.data[0..SIZE - self.capacity as usize] as &[u8]
    }

    pub fn archived_chunks(&self) -> &Vec<u32> {
        &self.archived_chunks
    }   
}

impl ByteDecodable for IndexingChunk {
    fn decode<R: Read>(read: &mut R) -> DecodeResult<Self> {
        let mut result: IndexingChunk;
        {
            let mut decoder = VByteDecoder::new(read.bytes());
            let capacity = try!(decoder.next().ok_or(DecodeError::MalformedInput));
            result = IndexingChunk {
                capacity: capacity as u16,
                data: unsafe { mem::uninitialized() },
            };
        }
        try!(read.read_exact(&mut result.data));
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
        let mut result: HotIndexingChunk;
        {
            let mut decoder = VByteDecoder::new(read.bytes());
            let capacity = try!(decoder.next().ok_or(DecodeError::MalformedInput));
            let last_doc_id = try!(decoder.next().ok_or(DecodeError::MalformedInput));
            let archived_chunks_len = try!(decoder.next().ok_or(DecodeError::MalformedInput));
            let mut archived_chunks = Vec::with_capacity(archived_chunks_len);
            for _ in 0..archived_chunks_len {
                archived_chunks.push(try!(decoder.next().ok_or(DecodeError::MalformedInput)) as u32);
            }
            result = HotIndexingChunk {
                capacity: capacity as u16,
                last_doc_id: last_doc_id as u64,
                archived_chunks: archived_chunks,
                data: unsafe { mem::uninitialized() },
            };
        }
        try!(read.read_exact(&mut result.data));
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
            VByteEncoded::new(self.archived_chunks.len()).write_to(write_ptr).unwrap();
            for i in &self.archived_chunks {
                VByteEncoded::new(*i as usize).write_to(write_ptr).unwrap();
            }
        }
        result.extend_from_slice(&self.data);
        result
    }
}
//TODO: Write meaningful tests
