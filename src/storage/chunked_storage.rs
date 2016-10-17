use std::mem;

use index::boolean_index::indexing_chunk::IndexingChunk;


pub struct ChunkedStorage {
    chunks: Vec<IndexingChunk>,
    reserved: u32,
}

impl ChunkedStorage {
    pub fn new(capacity: usize) -> Self {
        ChunkedStorage {
            reserved: 0,
            chunks: Vec::with_capacity(capacity),
        }
    }

    pub fn new_chunk(&mut self, id: u64) -> &mut IndexingChunk {
        self.reserved += 1;
        self.chunks.push(IndexingChunk {
            previous_chunk: 0,
            reserved_spot: id as u32,
            last_doc_id: 0,
            next_chunk: 0,
            postings_count: 0,
            capacity: 4092,
            data: unsafe { mem::uninitialized() },
        });
        &mut self.chunks[id as usize]
    }

    pub fn next_chunk(&mut self, id: u64) -> &mut IndexingChunk {
        let (last_reserved_spot, last_doc_id) = self.connect_chunk(id);
        let next = IndexingChunk {
            previous_chunk: last_reserved_spot,
            reserved_spot: self.reserved,
            next_chunk: 0,
            last_doc_id: last_doc_id,
            postings_count: 0,
            capacity: 4092,
            data: unsafe { mem::uninitialized() },
        };
        self.chunks.insert(self.reserved as usize, next);
        let old = self.reserved;
        self.reserved += 1;
        &mut self.chunks[old as usize]
    }

    fn connect_chunk(&mut self, id: u64) -> (u32, u64) {
        let mut pointer = id as usize;
        loop {
            let tmp = &mut self.chunks[pointer];
            pointer = tmp.next_chunk as usize;
            if pointer == 0 {
                tmp.next_chunk = self.reserved;
                return (tmp.reserved_spot, tmp.last_doc_id);
            }
        }
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.chunks.len()
    }

    #[inline]
    fn get(&self, id: u64) -> &IndexingChunk {
        &self.chunks[id as usize]
    }

    #[inline]
    fn get_chunk(&self, pos: usize) -> &IndexingChunk {
        &self.chunks[pos as usize]
    }


    #[inline]
    pub fn get_last(&self, id: u64) -> &IndexingChunk {
        let mut pointer = id as usize;
        loop {
            let tmp = &self.chunks[pointer];
            pointer = tmp.next_chunk as usize;
            if pointer == 0 {
                return tmp;
            }
        }
    }

    #[inline]
    fn get_mut(&mut self, id: u64) -> &mut IndexingChunk {
        &mut self.chunks[id as usize]
    }

    #[inline]
    fn get_chunk_mut(&mut self, pos: usize) -> &mut IndexingChunk {
        &mut self.chunks[pos as usize]
    }

    #[inline]
    pub fn get_last_mut(&mut self, id: u64) -> &mut IndexingChunk {
        let mut pointer = id as usize;
        loop {
            let old_p = pointer;
            pointer = self.chunks[pointer].next_chunk as usize;
            if pointer == 0 {
                return &mut self.chunks[old_p];
            }
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic() {
        let mut store = ChunkedStorage {
            reserved: 0,
            chunks: Vec::with_capacity(10),
        };
        {
            let chunk = store.new_chunk(0);
            let listing = vec![(0, vec![0, 10, 20]), (20, vec![24, 25, 289]), (204, vec![209, 2456])];
            chunk.append(&listing);
            assert_eq!(chunk.capacity, 4074);
            assert_eq!(chunk.postings_count, 3);
            assert_eq!(chunk.last_doc_id, 204);
        }
        let chunk = store.get(0);
        assert_eq!(chunk.capacity, 4074);
        assert_eq!(chunk.postings_count, 3);
        assert_eq!(chunk.last_doc_id, 204);
    }

    #[test]
    fn continued() {
        let mut store = ChunkedStorage {
            reserved: 0,
            chunks: Vec::with_capacity(10),
        };
        let listing = vec![(0, vec![0, 10, 20]), (20, vec![24, 25, 289]), (204, vec![209, 2456])];
        let next_listing = vec![(205, vec![0, 10, 20]), (225, vec![24, 25, 289]), (424, vec![209, 2456])];
        {
            let chunk = store.new_chunk(0);
            chunk.append(&listing);
            assert_eq!(chunk.capacity, 4074);
            assert_eq!(chunk.postings_count, 3);
            assert_eq!(chunk.last_doc_id, 204);
        }
        {
            let new_chunk = store.next_chunk(0);
            new_chunk.append(&next_listing);
            assert_eq!(new_chunk.capacity, 4074);
            assert_eq!(new_chunk.postings_count, 3);
            assert_eq!(new_chunk.last_doc_id, 424);
            assert_eq!(new_chunk.reserved_spot, 1);
        }
        let chunk = store.get(0);
        assert_eq!(chunk.next_chunk, 1);
        let new_chunk = store.get_chunk(chunk.next_chunk as usize);
        assert_eq!(new_chunk.capacity, 4074);
        assert_eq!(new_chunk.postings_count, 3);
        assert_eq!(new_chunk.last_doc_id, 424);
        assert_eq!(new_chunk.reserved_spot, 1);
        assert_eq!(chunk.capacity, 4074);
        assert_eq!(chunk.postings_count, 3);
        assert_eq!(chunk.last_doc_id, 204);
    }
}
