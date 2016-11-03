use std::io;
use std::io::Write;
use std::sync::Arc;
use std::fs::OpenOptions;
use std::path::Path;

use storage::{Storage, Result, ByteEncodable, ByteDecodable};
use chunked_storage::indexing_chunk::HotIndexingChunk;
pub use chunked_storage::indexing_chunk::IndexingChunk;


mod indexing_chunk;

pub const SIZE: usize = 104;
const HOTCHUNKS_FILENAME: &'static str = "hot_chunks.bin";
const ASSOCIATED_FILES: &'static [&'static str; 1] = &[HOTCHUNKS_FILENAME];


pub struct ChunkRef<'a> {
    read_ptr: usize,
    chunk: &'a mut HotIndexingChunk,
    archive: &'a mut Box<Storage<IndexingChunk>>,
}

// The idea here is the abstract the inner workings of chunked storage and indexing chunk from the index
// To do this, we implement Read and Write
impl<'a> io::Write for ChunkRef<'a> {
    // Fill the HotIndexingChunk
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let bytes_written = try!((&mut self.chunk.data[SIZE - self.chunk.capacity as usize..]).write(buf));
        self.chunk.capacity -= bytes_written as u16;
        if self.chunk.capacity == 0 {
            let id = self.archive.len();
            // TODO: Errorhandling
            self.archive.store(id as u64, self.chunk.archive(id as u32)).unwrap();
        }
        Ok(bytes_written)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> io::Read for ChunkRef<'a> {
    // BULLSHIT. RETHINK THIS!
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut bytes_read = 0;
        loop {
            let chunk_id = self.read_ptr / SIZE;
            if chunk_id < self.chunk.archived_chunks().len() {
                let read = (&self.archive.get(chunk_id as u64).unwrap().get_bytes()[self.read_ptr % SIZE..])
                    .read(&mut buf[bytes_read..])
                    .unwrap();
                if read == 0 {
                    return Ok(bytes_read);
                }
                bytes_read += read;
                self.read_ptr += read;
            } else {
                break;
            }
        }
        let read = (&self.chunk.get_bytes()[self.read_ptr % SIZE..]).read(&mut buf[bytes_read..]).unwrap();
        bytes_read += read;
        self.read_ptr += read;
        Ok(bytes_read)
    }
}

impl<'a> ChunkRef<'a> {
    #[inline]
    pub fn get_last_doc_id(&self) -> u64 {
        self.chunk.get_last_doc_id()
    }

    pub fn set_last_doc_id(&mut self, new_id: u64) {
        self.chunk.set_last_doc_id(new_id);
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

    fn chunk_ref(&mut self, term_id: u64) -> ChunkRef {
        ChunkRef {
            read_ptr: 0,
            chunk: &mut self.hot_chunks[term_id as usize],
            archive: &mut self.archive,
        }
    }

    pub fn new_chunk(&mut self, term_id: u64) -> ChunkRef {
        // The following code-uglyness is due to the fact that in indexing one sort thread can
        // overtake the other. That means: ids are not just comming in an incremental order but can jump

        // If the id is larger than the len (e.g. can not be just pushed)
        // Push uninitialized chunks until the desired chunk can be created and pushed
        let diff = term_id as usize - self.hot_chunks.len();
        for _ in 0..diff + 1 {
            self.hot_chunks.push(HotIndexingChunk::new());
        }

        self.hot_chunks[term_id as usize] = HotIndexingChunk::new();
        ChunkRef {
            read_ptr: 0,
            chunk: &mut self.hot_chunks[term_id as usize],
            archive: &mut self.archive,
        }
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
    pub fn get_current_mut(&mut self, id: u64) -> ChunkRef {
        ChunkRef {
            read_ptr: 0,
            chunk: &mut self.hot_chunks[id as usize],
            archive: &mut self.archive,
        }
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
    use std::io::{Read, Write};
    use utils::persistence::Volatile;
    use storage::RamStorage;

    // #[test]
    // fn basic() {
    //     let mut store = ChunkedStorage::new(10, Box::new(RamStorage::new()));
    //     {
    //         let chunk = store.new_chunk(0);
    //         let listing = vec![(0, vec![0, 10, 20]), (20, vec![24, 25, 289]), (204, vec![209, 2456])];
    //         chunk.append(&listing).unwrap();
    //         let data = chunk.get_bytes();
    //         assert_eq!(data.len(), 18);
    //     }

    //     let chunk = store.get_current(0);
    //     let data = chunk.get_bytes();
    //     assert_eq!(data.len(), 18);
    // }

    // #[test]
    // fn continued() {
    //     let mut store = ChunkedStorage::new(10, Box::new(RamStorage::new()));
    //     let listing = vec![(0, vec![0, 10, 20]), (20, vec![24, 25, 289]), (204, vec![209, 2456])];
    //     let next_listing = vec![(205, vec![0, 10, 20]), (225, vec![24, 25, 289]), (424, vec![209, 2456])];
    //     {
    //         let chunk = store.new_chunk(0);
    //         chunk.append(&listing).unwrap();
    //         assert_eq!(chunk.get_bytes().len(), 18);
    //     }
    //     {
    //         let new_chunk = store.next_chunk(0).unwrap();
    //         new_chunk.append(&next_listing).unwrap();
    //         assert_eq!(new_chunk.get_bytes().len(), 18);
    //     }
    //     let chunk = store.get_current(0);
    //     let old_chunk = store.get_archived((chunk.archived_chunks()[0]) as usize);
    //     assert_eq!(old_chunk.get_bytes().len(), 18);
    //     assert_eq!(chunk.get_bytes().len(), 18);
    // }

    #[test]
    fn overflowing_chunk_ref() {
        let mut store = ChunkedStorage::new(10, Box::new(RamStorage::new()));
        store.new_chunk(0);
        let data = (0..255u8).collect::<Vec<_>>();
        {
            let mut chunk_ref = store.chunk_ref(0);
            chunk_ref.write_all(&data);
        }
        {
            let chunk = store.get_current(0);
            let first_chunk = store.get_archived((chunk.archived_chunks()[0]) as usize);
            assert_eq!(first_chunk.get_bytes(), &data[0..SIZE]);
        }
        let mut read_data = Vec::new();
        {
            let mut chunk_ref = store.chunk_ref(0);
            chunk_ref.read_to_end(&mut read_data);
        }
        assert_eq!(data, read_data);
    }

    #[test]
    fn basic_chunk_ref() {
        let mut store = ChunkedStorage::new(10, Box::new(RamStorage::new()));
        store.new_chunk(0);
        let data = (0..20u8).collect::<Vec<_>>();
        {
            let mut chunk_ref = store.chunk_ref(0);
            chunk_ref.write_all(&data);
        }
        let mut read_data = Vec::new();
        {
            let mut chunk_ref = store.chunk_ref(0);
            chunk_ref.read_to_end(&mut read_data);
        }
        assert_eq!(data, read_data);
    }

    #[test]
    fn repeated_writes() {
        let mut store = ChunkedStorage::new(10, Box::new(RamStorage::new()));
        store.new_chunk(0);
        let data = (0..20u8).collect::<Vec<_>>();
        {
            let mut chunk_ref = store.chunk_ref(0);
            chunk_ref.write_all(&data[0..10]);
            chunk_ref.write_all(&data[10..20]);
        }
        let mut read_data = Vec::new();
        {
            let mut chunk_ref = store.chunk_ref(0);
            chunk_ref.read_to_end(&mut read_data);
        }
        assert_eq!(data, read_data);
    }

    #[test]
    fn repeated_writes_overflowing() {
        let mut store = ChunkedStorage::new(10, Box::new(RamStorage::new()));
        store.new_chunk(0);
        let data = (0..255u8).collect::<Vec<_>>();
        {
            let mut chunk_ref = store.chunk_ref(0);
            chunk_ref.write_all(&data);
            chunk_ref.write_all(&data);
            chunk_ref.write_all(&data);
        }
        let mut read_data = Vec::new();
        {
            let mut chunk_ref = store.chunk_ref(0);
            chunk_ref.read_to_end(&mut read_data);
        }
        assert_eq!(data, &read_data[0..255]);
        assert_eq!(data, &read_data[255..510]);
        assert_eq!(data, &read_data[510..765]);
    }
}
