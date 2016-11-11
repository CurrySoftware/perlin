use std::io;
use std::marker::PhantomData;

use utils::owning_iterator::SeekingIterator;

use storage::{ByteDecodable, Storage, StorageError};

use chunked_storage::indexing_chunk::{IndexingChunk, HotIndexingChunk};
use chunked_storage::SIZE;

pub struct MutChunkRef<'a> {
    chunk: &'a mut HotIndexingChunk,
    archive: &'a mut Box<Storage<IndexingChunk>>,
}

pub struct ChunkRef<'a> {
    read_ptr: usize,
    chunk: &'a HotIndexingChunk,
    archive: &'a Box<Storage<IndexingChunk>>,
}

// The idea here is the abstract the inner workings of
// chunked storage and indexing chunk from the index
// To do this, we implement Read and Write
impl<'a> io::Write for MutChunkRef<'a> {
    // Fill the HotIndexingChunk
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let bytes_written = try!((&mut self.chunk.data[SIZE - self.chunk.capacity as usize..]).write(buf));
        self.chunk.capacity -= bytes_written as u16;
        if self.chunk.capacity == 0 {
            let id = self.archive.len();

            match self.archive.store(id as u64, self.chunk.archive(id as u32)) {
                Ok(_) => {}
                Err(StorageError::IO(error)) |
                Err(StorageError::ReadError(Some(error))) |
                Err(StorageError::WriteError(Some(error))) => return Err(error),
                _ => return Err(io::Error::last_os_error()),
            }
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
            let chunk_index = self.read_ptr / SIZE;
            if chunk_index < self.chunk.archived_chunks().len() {
                let chunk_id = self.chunk.archived_chunks()[chunk_index];
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
    pub fn new(chunk: &'a HotIndexingChunk, archive: &'a Box<Storage<IndexingChunk>>) -> Self {
        ChunkRef {
            read_ptr: 0,
            chunk: chunk,
            archive: archive,
        }
    }

    #[inline]
    pub fn get_total_postings(&self) -> usize {
        self.chunk.get_total_postings()
    }
}

impl<'a> MutChunkRef<'a> {
    pub fn new(chunk: &'a mut HotIndexingChunk, archive: &'a mut Box<Storage<IndexingChunk>>) -> Self {
        MutChunkRef {
            chunk: chunk,
            archive: archive,
        }
    }    

    #[inline]
    pub fn increment_postings(&mut self, by: usize) {
        self.chunk.increment_postings(by);
    }

    #[inline]
    pub fn get_last_doc_id(&self) -> u64 {
        self.chunk.get_last_doc_id()
    }

    #[inline]
    pub fn set_last_doc_id(&mut self, new_id: u64) {
        self.chunk.set_last_doc_id(new_id);
    }
}
