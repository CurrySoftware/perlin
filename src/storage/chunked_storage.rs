use std;
use std::mem;
use std::fmt;
use std::io::{Write, Read};
use std::sync::Arc;
use std::fs::OpenOptions;
use std::path::Path;


use storage::{Storage, Result, ByteEncodable, ByteDecodable, DecodeResult, DecodeError};
use storage::compression::{VByteDecoder, VByteEncoded};
//use index::boolean_index::posting::{decode_from_chunk, Listing};

pub const SIZE: usize = 104;
pub const HOTCHUNKS_FILENAME: &'static str = "hot_chunks.bin";

pub struct IndexingChunk {
    // Currently the id of the archived chunk + 1. 0 thus means no predecessor
    previous_chunk: u32, // 4
    postings_count: u16, // 2
    capacity: u16, // 2
    last_doc_id: u64, // 8
    data: [u8; SIZE], //
}

impl PartialEq for IndexingChunk {
    fn eq(&self, other: &IndexingChunk) -> bool {
        self.previous_chunk == other.previous_chunk && self.postings_count == other.postings_count &&
        self.capacity == other.capacity && self.last_doc_id == other.last_doc_id &&
        self.data.as_ref() == other.data.as_ref()
    }
}

impl Eq for IndexingChunk {}

impl fmt::Debug for IndexingChunk {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        try!(write!(f,
                    "IndexingChunk, Previous: {}, Holds {} postings and has {} spare bytes! last_doc_id is {}\n",
                    self.previous_chunk,
                    self.postings_count,
                    self.capacity,
                    self.last_doc_id));
        try!(write!(f, "Data: {:?}", self.data.to_vec()));
        Ok(())
    }
}

impl IndexingChunk {
    pub fn new(previous: u32, last_doc_id: u64) -> Self {
        IndexingChunk {
            previous_chunk: previous,
            last_doc_id: last_doc_id,
            postings_count: 0,
            capacity: SIZE as u16,
            data: unsafe { mem::uninitialized() },
        }
    }

    pub fn previous_chunk(&self) -> Option<usize> {
        if self.previous_chunk == 0 {
            None
        } else {
            Some((self.previous_chunk - 1) as usize)
        }
    }

    pub fn get_bytes(&self) -> &[u8]
    {
        &self.data[0..SIZE - self.capacity as usize] as &[u8]
    }

    /// Adds listing to IndexingChunk. Returns Ok if listing fits into chunk
    /// Otherwise returns the posting number which did not fit into this chunk anymore
    pub fn append(&mut self, listing: &[(u64, Vec<u32>)]) -> std::result::Result<(), usize> {
        let mut working_slice = &mut self.data[SIZE - self.capacity as usize..];
        for (count, &(doc_id, ref positions)) in listing.iter().enumerate() {
            // println!("{}", count);
            let old_capa = self.capacity;
            let old_doc_id = self.last_doc_id;
            // Encode the doc_id as delta
            let delta_doc_id = doc_id - self.last_doc_id;
            // Encode the delta_doc_id with vbyte code
            let encoded_ddoc_id = VByteEncoded::new(delta_doc_id as usize);
            self.last_doc_id = doc_id;
            self.postings_count += 1;
            if let Ok(bytes_written) = encoded_ddoc_id.write_to(&mut working_slice) {
                // There was enough space. Count down capacity. Start encoding and writing positions
                self.capacity -= bytes_written as u16;
                // Encode positions len and add to data
                if let Ok(bytes_written) = VByteEncoded::new(positions.len()).write_to(&mut working_slice) {
                    self.capacity -= bytes_written as u16;
                } else {
                    self.capacity = old_capa;
                    self.last_doc_id = old_doc_id;
                    self.postings_count -= 1;
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
                        self.last_doc_id = old_doc_id;
                        self.postings_count -= 1;
                        return Err(count);
                    }
                }
            } else {
                self.capacity = old_capa;
                self.last_doc_id = old_doc_id;
                self.postings_count -= 1;
                return Err(count);
            }
        }
        Ok(())
    }
}

impl ByteDecodable for IndexingChunk {
    fn decode<R: Read>(read: &mut R) -> DecodeResult<Self> {
        let mut result: IndexingChunk;
        {
            let mut decoder = VByteDecoder::new(read.bytes());
            let previous_chunk = try!(decoder.next().ok_or(DecodeError::MalformedInput));
            let postings_count = try!(decoder.next().ok_or(DecodeError::MalformedInput));
            let capacity = try!(decoder.next().ok_or(DecodeError::MalformedInput));
            let last_doc_id = try!(decoder.next().ok_or(DecodeError::MalformedInput));
            result = IndexingChunk {
                previous_chunk: previous_chunk as u32,
                postings_count: postings_count as u16,
                capacity: capacity as u16,
                last_doc_id: last_doc_id as u64,
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
            VByteEncoded::new(self.previous_chunk as usize).write_to(write_ptr).unwrap();
            VByteEncoded::new(self.postings_count as usize).write_to(write_ptr).unwrap();
            VByteEncoded::new(self.capacity as usize).write_to(write_ptr).unwrap();
            VByteEncoded::new(self.last_doc_id as usize).write_to(write_ptr).unwrap();
        }
        result.extend_from_slice(&self.data);
        result
    }
}

pub struct ChunkedStorage {
    hot_chunks: Vec<IndexingChunk>, // Size of vocabulary
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
    pub fn persist(&self, target: &Path) -> Result<()>  {
        let mut file = try!(OpenOptions::new().write(true).create(true).truncate(true).open(target.join(HOTCHUNKS_FILENAME)));
        for chunk in &self.hot_chunks {
            let bytes = chunk.encode();
            try!(file.write(&bytes));
        }
        Ok(())
    }

    pub fn load(source: &Path, archive: Box<Storage<IndexingChunk>>) -> Result<Self> {
        let mut file = try!(OpenOptions::new().read(true).open(source.join(HOTCHUNKS_FILENAME)));
        let mut hot_chunks = Vec::new();
        while let Ok(decoded_chunk) = IndexingChunk::decode(&mut file) {
            hot_chunks.push(decoded_chunk);
        }
        Ok(ChunkedStorage{
            hot_chunks: hot_chunks,
            archive: archive
        })
    }

    pub fn new_chunk(&mut self, id: u64) -> &mut IndexingChunk {
        if id as usize > self.hot_chunks.len() {
            let diff = id as usize - self.hot_chunks.len();
            for _ in 0..diff {
                self.hot_chunks.push(unsafe { mem::uninitialized() });
            }
        } else if (id as usize) < self.hot_chunks.len() {
            self.hot_chunks[id as usize] = IndexingChunk::new(0, 0);
            return &mut self.hot_chunks[id as usize];
        }
        self.hot_chunks.push(IndexingChunk::new(0, 0));
        &mut self.hot_chunks[id as usize]
    }

    pub fn next_chunk(&mut self, id: u64) -> Result<&mut IndexingChunk> {
        let next = IndexingChunk::new(self.archive.len() as u32 + 1,
                                      self.hot_chunks[id as usize].last_doc_id);
        // TODO: Needs to go
        let new_id = self.archive.len() as u64;
        // That's more fun than I thought
        try!(self.archive.store(new_id,
                                mem::replace(&mut self.hot_chunks[id as usize], next)));
        Ok(&mut self.hot_chunks[id as usize])
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.hot_chunks.len()
    }

    #[inline]
    pub fn get_current(&self, id: u64) -> &IndexingChunk {
        &self.hot_chunks[id as usize]
    }

    #[inline]
    pub fn get_current_mut(&mut self, id: u64) -> &mut IndexingChunk {
        &mut self.hot_chunks[id as usize]
    }

    #[inline]
    pub fn get_archived(&self, pos: usize) -> Arc<IndexingChunk> {
        self.archive.get(pos as u64).unwrap()
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
            assert_eq!(chunk.postings_count, 3);
            assert_eq!(chunk.last_doc_id, 204);
        }
        let chunk = store.get_current(0);
        assert_eq!(chunk.capacity, (SIZE - 18) as u16);
        assert_eq!(chunk.postings_count, 3);
        assert_eq!(chunk.last_doc_id, 204);
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
            assert_eq!(chunk.postings_count, 3);
            assert_eq!(chunk.last_doc_id, 204);
        }
        {
            let new_chunk = store.next_chunk(0).unwrap();
            new_chunk.append(&next_listing).unwrap();
            assert_eq!(new_chunk.capacity, (SIZE - 18) as u16);
            assert_eq!(new_chunk.postings_count, 3);
            assert_eq!(new_chunk.last_doc_id, 424);
        }
        let chunk = store.get_current(0);
        let old_chunk = store.get_archived((chunk.previous_chunk - 1) as usize);
        assert_eq!(old_chunk.capacity, (SIZE - 18) as u16);
        assert_eq!(old_chunk.postings_count, 3);
        assert_eq!(old_chunk.last_doc_id, 204);
        assert_eq!(chunk.capacity, (SIZE - 18) as u16);
        assert_eq!(chunk.postings_count, 3);
        assert_eq!(chunk.last_doc_id, 424);
    }

    #[test]
    fn chunk_basic() {
        let mut chunk = IndexingChunk::new(0, 0);

        let listing = vec![(0, vec![0, 10, 20]), (20, vec![24, 25, 289]), (204, vec![209, 2456])];
        chunk.append(&listing).unwrap();
        println!("{:?}", chunk);
        assert_eq!(chunk.capacity, (SIZE - 18) as u16);
        assert_eq!(chunk.postings_count, 3);
        assert_eq!(chunk.last_doc_id, 204);
    }

    #[test]
    fn full() {
        let mut listing = (0..SIZE / 3).map(|i| (i as u64, vec![0 as u32])).collect::<Vec<_>>();
        let mut additional = (0u32..(SIZE % 3) as u32).collect::<Vec<_>>();
        listing[0].1.append(&mut additional);
        let mut chunk = IndexingChunk::new(0, 0);
        assert_eq!(chunk.append(&listing), Ok(()));
        assert_eq!(chunk.postings_count, (SIZE / 3) as u16);
        assert_eq!(chunk.capacity, 0);
    }

    #[test]
    fn overflowing_single() {
        let listing = vec![(0, (0..10000).collect::<Vec<_>>())];
        let mut chunk = IndexingChunk::new(0, 0);
        assert_eq!(chunk.append(&listing), Err(0));
        assert_eq!(chunk.capacity, SIZE as u16);
        assert_eq!(chunk.postings_count, 0);
    }

    #[test]
    fn overflowing_second() {
        let mut listing = (0..SIZE / 3 - 1).map(|i| (i as u64, vec![0 as u32])).collect::<Vec<_>>();
        let mut additional = (0u32..(SIZE % 3) as u32).collect::<Vec<_>>();
        listing[0].1.append(&mut additional);
        let mut chunk = IndexingChunk::new(0, 0);
        assert_eq!(chunk.append(&listing), Ok(()));
        assert_eq!(chunk.postings_count, (SIZE / 3 - 1) as u16);
        assert_eq!(chunk.last_doc_id, (SIZE / 3 - 2) as u64);
        let listing = vec![((SIZE / 3 - 1) as u64, vec![0]), ((SIZE / 3) as u64, (0..10000).collect::<Vec<_>>())];
        assert_eq!(chunk.append(&listing), Err(1));
        assert_eq!(chunk.postings_count, (SIZE / 3) as u16);
        assert_eq!(chunk.last_doc_id, (SIZE / 3 - 1) as u64);
    }

    #[test]
    fn encode_indexing_chunk() {
        let mut chunk = IndexingChunk::new(0, 0);
        let listing = vec![(0, vec![0, 10, 20]), (20, vec![24, 25, 289]), (204, vec![209, 2456])];
        chunk.append(&listing).unwrap();
        let bytes = chunk.encode();
        let decoded_chunk = IndexingChunk::decode(&mut bytes.as_slice()).unwrap();
        assert_eq!(chunk, decoded_chunk);
    }
}
