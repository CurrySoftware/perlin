use std;
use std::mem;
use std::fmt;
use std::io::{Write, Read};
use std::sync::Arc;
use std::fs::OpenOptions;
use std::path::Path;


use storage::{Storage, Result, ByteEncodable, ByteDecodable, DecodeResult, DecodeError};
use storage::compression::{VByteDecoder, VByteEncoded};

pub const SIZE: usize = 104;
const HOTCHUNKS_FILENAME: &'static str = "hot_chunks.bin";
const ASSOCIATED_FILES: &'static [&'static str; 1] = &[HOTCHUNKS_FILENAME];


pub struct HotIndexingChunk {
    capacity: u16,
    last_doc_id: u64,
    // TODO: Can we remove that indirection?
    archived_chunks: Vec<u32>,
    data: [u8; SIZE],
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
        try!(write!(f, "Data: {:?}", self.data.to_vec()));
        Ok(())
    }
}

// TODO: Complete!
impl fmt::Debug for HotIndexingChunk {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        try!(write!(f, "HotIndexingChunk has {} spare bytes!\n", self.capacity));
        try!(write!(f, "Data: {:?}", self.data.to_vec()));
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


impl HotIndexingChunk {
    pub fn new() -> Self {
        HotIndexingChunk {
            last_doc_id: 0,
            capacity: SIZE as u16,
            archived_chunks: Vec::new(),
            data: unsafe { mem::uninitialized() },
        }
    }

    fn archive(&mut self, at: u32) -> IndexingChunk {
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

    /// Adds listing to IndexingChunk. Returns Ok if listing fits into chunk
    /// Otherwise returns the posting number which did not fit into this chunk anymore
    pub fn append(&mut self, listing: &[(u64, Vec<u32>)]) -> std::result::Result<(), usize> {
        let mut working_slice = &mut self.data[SIZE - self.capacity as usize..];
        for (count, &(doc_id, ref positions)) in listing.iter().enumerate() {
            // println!("{}", count);
            let old_capa = self.capacity;
            // Encode the doc_id as delta
            let delta_doc_id = doc_id - self.last_doc_id;
            // Encode the delta_doc_id with vbyte code
            let encoded_ddoc_id = VByteEncoded::new(delta_doc_id as usize);
            if let Ok(bytes_written) = encoded_ddoc_id.write_to(&mut working_slice) {
                // There was enough space. Count down capacity. Start encoding and writing positions
                self.capacity -= bytes_written as u16;
                // Encode positions len and add to data
                if let Ok(bytes_written) = VByteEncoded::new(positions.len()).write_to(&mut working_slice) {
                    self.capacity -= bytes_written as u16;
                } else {
                    self.capacity = old_capa;
                    return Err(count);
                }
                // Encode positions and add to data
                let mut last_position = 0;
                for position in positions {
                    if let Ok(bytes_written) = VByteEncoded::new(*position as usize - last_position)
                        .write_to(&mut working_slice) {
                        self.capacity -= bytes_written as u16;
                        last_position = *position as usize;
                    } else {
                        self.capacity = old_capa;
                        return Err(count);
                    }
                }
            } else {
                self.capacity = old_capa;
                return Err(count);
            }
            //TODO: Check for performance impact
            self.last_doc_id += delta_doc_id;
        }
        Ok(())
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

// TODO: Think about implementing `Persistent` and `Volatile`
pub struct ChunkedStorage {
    hot_chunks: Vec<HotIndexingChunk>, // Size of vocabulary
    archive: Box<Storage<IndexingChunk>>,
}

impl ChunkedStorage {
    pub fn new(capacity: usize, archive: Box<Storage<IndexingChunk>>) -> Self {
        ChunkedStorage {
            hot_chunks: Vec::with_capacity(capacity),
            archive: archive,
        }
    }

    /// Persists hot_chunks to a file.
    /// We currently only need to persist hot_chunks
    /// Archive takes care of the rest
    pub fn persist(&self, target: &Path) -> Result<()> {
        let mut file =
            try!(OpenOptions::new().write(true).create(true).truncate(true).open(target.join(HOTCHUNKS_FILENAME)));
        for chunk in &self.hot_chunks {
            let bytes = chunk.encode();
            try!(file.write(&bytes));
        }
        Ok(())
    }

    pub fn load(source: &Path, archive: Box<Storage<IndexingChunk>>) -> Result<Self> {
        let mut file = try!(OpenOptions::new().read(true).open(source.join(HOTCHUNKS_FILENAME)));
        let mut hot_chunks = Vec::new();
        while let Ok(decoded_chunk) = HotIndexingChunk::decode(&mut file) {
            hot_chunks.push(decoded_chunk);
        }
        Ok(ChunkedStorage {
            hot_chunks: hot_chunks,
            archive: archive,
        })
    }

    pub fn new_chunk(&mut self, term_id: u64) -> &mut HotIndexingChunk {
        // The following code-uglyness is due to the fact that in indexing one sort thread can
        // overtake the other. That means: ids are not just comming in an incremental order but can jump

        // If the id is larger than the len (e.g. can not be just pushed)
        // Push uninitialized chunks until the desired chunk can be created and pushed
        let diff = term_id as usize - self.hot_chunks.len();
        for _ in 0..diff + 1 {
            self.hot_chunks.push(HotIndexingChunk::new());
        }

        self.hot_chunks[term_id as usize] = HotIndexingChunk::new();
        return &mut self.hot_chunks[term_id as usize];
    }

    pub fn next_chunk(&mut self, id: u64) -> Result<&mut HotIndexingChunk> {
        let to_archive = self.hot_chunks[id as usize].archive(self.archive.len() as u32);
        // TODO: Needs to go
        let new_id = self.archive.len() as u64;
        // That's more fun than I thought
        try!(self.archive.store(new_id, to_archive));
        Ok(&mut self.hot_chunks[id as usize])
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.hot_chunks.len()
    }

    #[inline]
    pub fn get_current(&self, id: u64) -> &HotIndexingChunk {
        &self.hot_chunks[id as usize]
    }

    #[inline]
    pub fn get_current_mut(&mut self, id: u64) -> &mut HotIndexingChunk {
        &mut self.hot_chunks[id as usize]
    }

    #[inline]
    pub fn get_archived(&self, pos: usize) -> Arc<IndexingChunk> {
        self.archive.get(pos as u64).unwrap()
    }

    pub fn associated_files() -> &'static [&'static str] {
        ASSOCIATED_FILES
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use utils::persistence::Volatile;
    use storage::{RamStorage, ByteEncodable, ByteDecodable};

    #[test]
    fn basic() {
        let mut store = ChunkedStorage::new(10, Box::new(RamStorage::new()));
        {
            let chunk = store.new_chunk(0);
            let listing = vec![(0, vec![0, 10, 20]), (20, vec![24, 25, 289]), (204, vec![209, 2456])];
            chunk.append(&listing).unwrap();
            assert_eq!(chunk.capacity, (SIZE - 18) as u16);
        }

        let chunk = store.get_current(0);
        assert_eq!(chunk.capacity, (SIZE - 18) as u16);
    }

    #[test]
    fn continued() {
        let mut store = ChunkedStorage::new(10, Box::new(RamStorage::new()));
        let listing = vec![(0, vec![0, 10, 20]), (20, vec![24, 25, 289]), (204, vec![209, 2456])];
        let next_listing = vec![(205, vec![0, 10, 20]), (225, vec![24, 25, 289]), (424, vec![209, 2456])];
        {
            let chunk = store.new_chunk(0);
            chunk.append(&listing).unwrap();
            assert_eq!(chunk.capacity, (SIZE - 18) as u16);
        }
        {
            let new_chunk = store.next_chunk(0).unwrap();
            new_chunk.append(&next_listing).unwrap();
            assert_eq!(new_chunk.capacity, (SIZE - 18) as u16);
        }
        let chunk = store.get_current(0);
        let old_chunk = store.get_archived((chunk.archived_chunks[0]) as usize);
        assert_eq!(old_chunk.capacity, (SIZE - 18) as u16);
        assert_eq!(chunk.capacity, (SIZE - 18) as u16);
    }

    #[test]
    fn chunk_basic() {
        let mut chunk = HotIndexingChunk::new();

        let listing = vec![(0, vec![0, 10, 20]), (20, vec![24, 25, 289]), (204, vec![209, 2456])];
        chunk.append(&listing).unwrap();
        assert_eq!(chunk.capacity, (SIZE - 18) as u16);
    }

    #[test]
    fn full() {
        let mut listing = (0..SIZE / 3).map(|i| (i as u64, vec![0 as u32])).collect::<Vec<_>>();
        let mut additional = (0u32..(SIZE % 3) as u32).collect::<Vec<_>>();
        listing[0].1.append(&mut additional);
        let mut chunk = HotIndexingChunk::new();
        assert_eq!(chunk.append(&listing), Ok(()));
        assert_eq!(chunk.capacity, 0);
    }

    #[test]
    fn overflowing_single() {
        let listing = vec![(0, (0..10000).collect::<Vec<_>>())];
        let mut chunk = HotIndexingChunk::new();
        assert_eq!(chunk.append(&listing), Err(0));
        assert_eq!(chunk.capacity, SIZE as u16);
    }

    #[test]
    fn overflowing_second() {
        let mut listing = (0..SIZE / 3 - 1).map(|i| (i as u64, vec![0 as u32])).collect::<Vec<_>>();
        let mut additional = (0u32..(SIZE % 3) as u32).collect::<Vec<_>>();
        listing[0].1.append(&mut additional);
        let mut chunk = HotIndexingChunk::new();
        assert_eq!(chunk.append(&listing), Ok(()));
        let listing = vec![((SIZE / 3 - 1) as u64, vec![0]), ((SIZE / 3) as u64, (0..10000).collect::<Vec<_>>())];
        assert_eq!(chunk.append(&listing), Err(1));
    }

    #[test]
    fn encode_indexing_chunk() {
        let mut chunk = HotIndexingChunk::new();
        let listing = vec![(0, vec![0, 10, 20]), (20, vec![24, 25, 289]), (204, vec![209, 2456])];
        chunk.append(&listing).unwrap();
        let bytes = chunk.encode();
        let decoded_chunk = HotIndexingChunk::decode(&mut bytes.as_slice()).unwrap();
        assert_eq!(chunk, decoded_chunk);
    }
}
