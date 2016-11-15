use std::io;
use std::io::Write;

use storage::{Storage, StorageError};

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
    // TODO: Improve and comment this
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let mut bytes_read = 0;
        loop {
            let chunk_index = self.read_ptr / SIZE;
            if chunk_index < self.chunk.archived_chunks().len() {
                let chunk_id = self.chunk.archived_chunks()[chunk_index].2;
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

//Heavily "inspired" by std::io::Cursor::seek
impl<'a> io::Seek for ChunkRef<'a> {
    fn seek(&mut self, style: io::SeekFrom) -> io::Result<u64> {
        use std::io::SeekFrom;
       let pos = match style {
            SeekFrom::Start(n) => { self.read_ptr = n as usize; return Ok(n) }
            SeekFrom::End(n) => self.bytes_len() as i64 + n,
            SeekFrom::Current(n) => self.read_ptr as i64 + n,
        };

        if pos < 0 {
            Err(io::Error::new(io::ErrorKind::InvalidInput,
                           "invalid seek to a negative position"))
        } else {
            self.read_ptr = pos as usize;
            Ok(self.read_ptr as u64)
        }
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
    pub fn bytes_len(&self) -> usize {
        (self.chunk.archived_chunks().len() + 1) * SIZE - self.chunk.capacity as usize
    }

    #[inline]
    pub fn get_total_postings(&self) -> usize {
        self.chunk.get_total_postings()
    }

    #[inline]
    pub fn doc_id_offset(&self, doc_id: &u64) -> (u64, usize) {
        self.chunk.doc_id_offset(doc_id)
    }
}

impl<'a> MutChunkRef<'a> {
    pub fn new(chunk: &'a mut HotIndexingChunk, archive: &'a mut Box<Storage<IndexingChunk>>) -> Self {
        MutChunkRef {
            chunk: chunk,
            archive: archive,
        }
    }

    pub fn write_posting(&mut self, doc_id: u64, buf: &[u8]) -> io::Result<()>{
        self.chunk.add_doc_id(doc_id);
        self.write_all(buf)?;
        Ok(())
    }
    
    #[inline]
    pub fn get_last_doc_id(&self) -> u64 {
        self.chunk.get_last_doc_id()
    }
}


#[cfg(test)]
mod tests {
    use std::io::Read;
    
    use utils::persistence::Volatile;
    use chunked_storage::ChunkedStorage;
    use storage::RamStorage;
    
    #[test]
    fn write_posting_basic() {
        let mut storage = ChunkedStorage::new(0, Box::new(RamStorage::new()));
        {
            let mut chunk = storage.new_chunk(0);
            let buf = vec![1; 10];
            chunk.write_posting(0, &buf).unwrap();
        }
        
        let mut buf = Vec::new();
        let mut chunk = storage.get(0);
        chunk.read_to_end(&mut buf).unwrap();
        assert_eq!(buf, vec![1; 10]);
    }

    #[test]
    fn write_posting_overflowing() {
        let mut storage = ChunkedStorage::new(0, Box::new(RamStorage::new()));
        {
            let mut chunk = storage.new_chunk(0);
            let buf = vec![1; 1000];
            chunk.write_posting(0, &buf).unwrap();
        }
        
        let mut buf = Vec::new();
        let mut chunk = storage.get(0);
        chunk.read_to_end(&mut buf).unwrap();
        assert_eq!(buf, vec![1; 1000]);
    }

}
