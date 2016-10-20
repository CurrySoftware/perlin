use std::mem;
use std::io::Read;

use storage::compression::VByteDecoder;
use storage::ByteDecodable;

use index::boolean_index::posting::{decode_from_chunk, Listing};
use index::boolean_index::indexing_chunk::{SIZE, IndexingChunk};

#[derive(Debug)]
pub struct ChunkedStorage {
    hot_chunks: Vec<IndexingChunk>, // Size of vocabulary
    archived_chunks: Vec<IndexingChunk>,
}

impl ChunkedStorage {
    pub fn new(capacity: usize) -> Self {
        ChunkedStorage {
            hot_chunks: Vec::with_capacity(capacity),
            archived_chunks: Vec::with_capacity(capacity / 10),
        }
    }

    pub fn new_chunk(&mut self, id: u64) -> &mut IndexingChunk {
        if id as usize > self.hot_chunks.len() {
            let diff = id as usize - self.hot_chunks.len();
            for _ in 0..diff {
                self.hot_chunks.push(unsafe { mem::uninitialized() });
            }
        } else if (id as usize) < self.hot_chunks.len() {
            self.hot_chunks[id as usize] = IndexingChunk {
                previous_chunk: 0,
                reserved_spot: id as u32,
                last_doc_id: 0,
                next_chunk: 0,
                postings_count: 0,
                capacity: SIZE as u16,
                data: unsafe { mem::uninitialized() },
            };
            return &mut self.hot_chunks[id as usize];
        }
        self.hot_chunks.push(IndexingChunk {
            previous_chunk: 0,
            reserved_spot: id as u32,
            last_doc_id: 0,
            next_chunk: 0,
            postings_count: 0,
            capacity: SIZE as u16,
            data: unsafe { mem::uninitialized() },
        });
        &mut self.hot_chunks[id as usize]
    }

    pub fn next_chunk(&mut self, id: u64) -> &mut IndexingChunk {
        let next = IndexingChunk {
            previous_chunk: self.archived_chunks.len() as u32 + 1,
            reserved_spot: id as u32,
            next_chunk: 0,
            last_doc_id: self.hot_chunks[id as usize].last_doc_id,
            postings_count: 0,
            capacity: SIZE as u16,
            data: unsafe { mem::uninitialized() },
        };
        // That's more fun than I thought
        self.archived_chunks.push(mem::replace(&mut self.hot_chunks[id as usize], next));
        &mut self.hot_chunks[id as usize]
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
    pub fn get_archived(&self, pos: usize) -> &IndexingChunk {
        &self.archived_chunks[pos]
    }

    #[inline]
    pub fn get_archived_mut(&mut self, pos: usize) -> &IndexingChunk {
        &mut self.archived_chunks[pos]
    }

    pub fn decode_postings(&self, id: u64) -> Option<Listing> {
        if self.hot_chunks.len() < id as usize {
            return None;
        }
        let mut chunk = self.get_current(id);
        let mut listing = decode_from_chunk(&mut (&chunk.data[0..SIZE - chunk.capacity as usize] as &[u8])).unwrap();
        loop {
//            println!("Loop {}", chunk.previous_chunk);
            if chunk.previous_chunk != 0 {
                chunk = self.get_archived((chunk.previous_chunk - 1) as usize);
                match decode_from_chunk(&mut (&chunk.data[0..SIZE - chunk.capacity as usize] as &[u8])) {
                   Ok(mut new) => { new.append(&mut listing);
                                listing = new;
                   }
                    Err((doc_id, position)) => {
                        println!("{}-{}", doc_id, position);
                        println!("{:?}", chunk);
                        panic!("TF");
                    }
                } 
            } else {
                return Some(listing);
            }
        }
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic() {
        let mut store = ChunkedStorage::new(10);
        {
            let chunk = store.new_chunk(0);
            let listing = vec![(0, vec![0, 10, 20]), (20, vec![24, 25, 289]), (204, vec![209, 2456])];
            chunk.append(&listing).unwrap();
            assert_eq!(chunk.capacity, 4074);
            assert_eq!(chunk.postings_count, 3);
            assert_eq!(chunk.last_doc_id, 204);
        }
        let chunk = store.get_current(0);
        assert_eq!(chunk.capacity, 4074);
        assert_eq!(chunk.postings_count, 3);
        assert_eq!(chunk.last_doc_id, 204);
    }

    #[test]
    fn continued() {
        let mut store = ChunkedStorage::new(10);
        let listing = vec![(0, vec![0, 10, 20]), (20, vec![24, 25, 289]), (204, vec![209, 2456])];
        let next_listing = vec![(205, vec![0, 10, 20]), (225, vec![24, 25, 289]), (424, vec![209, 2456])];
        {
            let chunk = store.new_chunk(0);
            chunk.append(&listing).unwrap();
            assert_eq!(chunk.capacity, 4074);
            assert_eq!(chunk.postings_count, 3);
            assert_eq!(chunk.last_doc_id, 204);
        }
        {
            let new_chunk = store.next_chunk(0);
            new_chunk.append(&next_listing).unwrap();
            assert_eq!(new_chunk.capacity, 4074);
            assert_eq!(new_chunk.postings_count, 3);
            assert_eq!(new_chunk.last_doc_id, 424);
            assert_eq!(new_chunk.reserved_spot, 1);
        }
        let chunk = store.get_current(0);
        assert_eq!(chunk.next_chunk, 0);
        let new_chunk = store.get_archived(chunk.next_chunk as usize);
        assert_eq!(new_chunk.capacity, 4074);
        assert_eq!(new_chunk.postings_count, 3);
        assert_eq!(new_chunk.last_doc_id, 424);
        assert_eq!(new_chunk.reserved_spot, 1);
        assert_eq!(chunk.capacity, 4074);
        assert_eq!(chunk.postings_count, 3);
        assert_eq!(chunk.last_doc_id, 204);
    }
}
