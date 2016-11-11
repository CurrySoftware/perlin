use std::io::Write;
use std::fs::OpenOptions;
use std::path::Path;

use storage::{Storage, Result, ByteEncodable, ByteDecodable};
use chunked_storage::indexing_chunk::HotIndexingChunk;
use chunked_storage::chunk_ref::{ChunkIter, ChunkRef, MutChunkRef};
pub use chunked_storage::indexing_chunk::IndexingChunk;


mod indexing_chunk;
pub mod chunk_ref;

pub const SIZE: usize = 222;
const HOTCHUNKS_FILENAME: &'static str = "hot_chunks.bin";
const ASSOCIATED_FILES: &'static [&'static str; 1] = &[HOTCHUNKS_FILENAME];


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

    pub fn new_chunk(&mut self, term_id: u64) -> MutChunkRef {
        // The following code-uglyness is due to the fact that in indexing one sort thread can
        // overtake the other. That means: ids are not just comming in an incremental order but can jump

        // If the id is larger than the len (e.g. can not be just pushed)
        // Push uninitialized chunks until the desired chunk can be created and pushed
        let diff = term_id as usize - self.hot_chunks.len();
        for _ in 0..diff + 1 {
            self.hot_chunks.push(HotIndexingChunk::new());
        }

        self.hot_chunks[term_id as usize] = HotIndexingChunk::new();
        MutChunkRef::new(&mut self.hot_chunks[term_id as usize], &mut self.archive)
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.hot_chunks.len()
    }

    #[inline]
    pub fn get(&self, id: u64) -> ChunkRef {
        ChunkRef::new(&self.hot_chunks[id as usize], &self.archive)        
    }

    #[inline]
    pub fn get_iter<T: ByteDecodable>(&self, id: u64) -> ChunkIter<T> {
        ChunkIter::new(self.get(id))
    }

    #[inline]
    pub fn get_mut(&mut self, id: u64) -> MutChunkRef {
        MutChunkRef::new(&mut self.hot_chunks[id as usize], &mut self.archive)        
    }


    pub fn associated_files() -> &'static [&'static str] {
        ASSOCIATED_FILES
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use utils::persistence::Volatile;
    use storage::RamStorage;

    #[test]
    fn overflowing_chunk_ref() {
        let mut store = ChunkedStorage::new(10, Box::new(RamStorage::new()));
        store.new_chunk(0);
        let data = (0..255u8).collect::<Vec<_>>();
        {
            let mut chunk_ref = store.get_mut(0);
            chunk_ref.write_all(&data).unwrap();
        }
        let mut read_data = Vec::new();
        {
            let mut chunk_ref = store.get(0);
            chunk_ref.read_to_end(&mut read_data).unwrap();
        }
        assert_eq!(data, read_data);
    }

    #[test]
    fn basic_chunk_ref() {
        let mut store = ChunkedStorage::new(10, Box::new(RamStorage::new()));
        store.new_chunk(0);
        let data = (0..20u8).collect::<Vec<_>>();
        {
            let mut chunk_ref = store.get_mut(0);
            chunk_ref.write_all(&data).unwrap();
        }
        let mut read_data = Vec::new();
        {
            let mut chunk_ref = store.get(0);
            chunk_ref.read_to_end(&mut read_data).unwrap();
        }
        assert_eq!(data, read_data);
    }

    #[test]
    fn repeated_writes() {
        let mut store = ChunkedStorage::new(10, Box::new(RamStorage::new()));
        store.new_chunk(0);
        let data = (0..20u8).collect::<Vec<_>>();
        {
            let mut chunk_ref = store.get_mut(0);
            chunk_ref.write_all(&data[0..10]).unwrap();
            chunk_ref.write_all(&data[10..20]).unwrap();
        }
        let mut read_data = Vec::new();
        {
            let mut chunk_ref = store.get(0);
            chunk_ref.read_to_end(&mut read_data).unwrap();
        }
        assert_eq!(data, read_data);
    }

    #[test]
    fn repeated_writes_overflowing() {
        let mut store = ChunkedStorage::new(10, Box::new(RamStorage::new()));
        store.new_chunk(0);
        let data = (0..255u8).collect::<Vec<_>>();
        {
            let mut chunk_ref = store.get_mut(0);
            chunk_ref.write_all(&data).unwrap();
            chunk_ref.write_all(&data).unwrap();
            chunk_ref.write_all(&data).unwrap();
        }
        let mut read_data = Vec::new();
        {
            let mut chunk_ref = store.get(0);
            chunk_ref.read_to_end(&mut read_data).unwrap();
        }
        assert_eq!(data, &read_data[0..255]);
        assert_eq!(data, &read_data[255..510]);
        assert_eq!(data, &read_data[510..765]);
    }

    #[test]
    fn multiple_chunks() {
        let mut store = ChunkedStorage::new(10, Box::new(RamStorage::new()));
        for i in 0..100u8 {
            let data = (0..4096).map(|_| i).collect::<Vec<_>>();
            let mut chunk_ref = store.new_chunk(i as u64);
            chunk_ref.write_all(&data).unwrap();
        }
        for i in 0..100u8 {
            let data = (0..4096).map(|_| i).collect::<Vec<_>>();
            let mut chunk_ref = store.get(i as u64);
            let mut read_data = Vec::new();
            chunk_ref.read_to_end(&mut read_data).unwrap();
            assert_eq!(data, read_data);
        }
    }

    #[test]
    fn repeated_reads() {
        let mut store = ChunkedStorage::new(10, Box::new(RamStorage::new()));
        store.new_chunk(0);
        let data = (0..20u8).collect::<Vec<_>>();
        {
            let mut chunk_ref = store.get_mut(0);
            chunk_ref.write_all(&data).unwrap();
        }
        let mut read_data = [0u8; 10];
        {
            let mut chunk_ref = store.get(0);
            chunk_ref.read(&mut read_data).unwrap();
            assert_eq!(read_data, [0, 1, 2, 3, 4, 5, 6, 7, 8, 9]);
            chunk_ref.read(&mut read_data).unwrap();
            assert_eq!(read_data, [10, 11, 12, 13, 14, 15, 16, 17, 18, 19]);
        }
    }

    #[test]
    fn repeated_reads_overflowing() {
        let mut store = ChunkedStorage::new(10, Box::new(RamStorage::new()));
        store.new_chunk(0);
        let data = (0..1000u32).map(|i| (i % 255) as u8).collect::<Vec<_>>();
        {
            let mut chunk_ref = store.get_mut(0);
            chunk_ref.write_all(&data).unwrap();
        }
        let mut read_data = [0u8; 10];
        {
            let mut chunk_ref = store.get(0);
            for i in 0..100 {
                chunk_ref.read(&mut read_data).unwrap();
                assert_eq!(read_data.to_vec(),
                           (i * 10..i * 10 + 10).map(|i| (i % 255) as u8).collect::<Vec<_>>());
            }
        }
    }
}
