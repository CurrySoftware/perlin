use std::mem;

use index::boolean_index::indexing_chunk::{SIZE, IndexingChunk};

#[derive(Debug)]
pub struct ChunkedStorage {
    hot_chunks: Vec<IndexingChunk>, //Size of vocabulary
    archived_chunks: Vec<IndexingChunk>, 
    reserved: u32,
    archive_count: u32,
}

impl ChunkedStorage {
    pub fn new(capacity: usize) -> Self {
        ChunkedStorage {
            reserved: 0,
            archive_count: 0,
            hot_chunks: Vec::with_capacity(capacity),
            archived_chunks: Vec::with_capacity(capacity/10),
        }
    }

    pub fn new_chunk(&mut self, id: u64) -> &mut IndexingChunk {
        self.reserved += 1;
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
            previous_chunk: self.archived_chunks.len() as u32,
            reserved_spot: id as u32,
            next_chunk: 0,
            last_doc_id: self.hot_chunks[id as usize].last_doc_id,
            postings_count: 0,
            capacity: SIZE as u16,
            data: unsafe { mem::uninitialized() },
        };
        //That's more fun than I thought
        self.archived_chunks.push(mem::replace(&mut self.hot_chunks[id as usize], next));
        &mut self.hot_chunks[id as usize]
    }    

    #[inline]
    pub fn len(&self) -> usize {
        self.hot_chunks.len()
    }

    #[inline]
    fn get(&self, id: u64) -> &IndexingChunk {
        &self.hot_chunks[id as usize]
    }


    #[inline]
    pub fn get_current(&self, id: u64) -> &IndexingChunk {
        &self.hot_chunks[id as usize]
    }

    #[inline]
    fn get_mut(&mut self, id: u64) -> &mut IndexingChunk {
        &mut self.hot_chunks[id as usize]
    }

    #[inline]
    pub fn get_current_mut(&mut self, id: u64) -> &mut IndexingChunk {
        &mut self.hot_chunks[id as usize]
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    // #[test]
    // fn basic() {
    //     let mut store = ChunkedStorage {
    //         reserved: 0,
    //         hot_chunks: Vec::with_capacity(10),
    //         archived_chunks: Vec::with_capacity(10)
    //     };
    //     {
    //         let chunk = store.new_chunk(0);
    //         let listing = vec![(0, vec![0, 10, 20]), (20, vec![24, 25, 289]), (204, vec![209, 2456])];
    //         chunk.append(&listing);
    //         assert_eq!(chunk.capacity, 4074);
    //         assert_eq!(chunk.postings_count, 3);
    //         assert_eq!(chunk.last_doc_id, 204);
    //     }
    //     let chunk = store.get(0);
    //     assert_eq!(chunk.capacity, 4074);
    //     assert_eq!(chunk.postings_count, 3);
    //     assert_eq!(chunk.last_doc_id, 204);
    // }

    // #[test]
    // fn continued() {
    //     let mut store = ChunkedStorage {
    //         reserved: 0,
    //         chunks: Vec::with_capacity(10),
    //     };
    //     let listing = vec![(0, vec![0, 10, 20]), (20, vec![24, 25, 289]), (204, vec![209, 2456])];
    //     let next_listing = vec![(205, vec![0, 10, 20]), (225, vec![24, 25, 289]), (424, vec![209, 2456])];
    //     {
    //         let chunk = store.new_chunk(0);
    //         chunk.append(&listing);
    //         assert_eq!(chunk.capacity, 4074);
    //         assert_eq!(chunk.postings_count, 3);
    //         assert_eq!(chunk.last_doc_id, 204);
    //     }
    //     {
    //         let new_chunk = store.next_chunk(0);
    //         new_chunk.append(&next_listing);
    //         assert_eq!(new_chunk.capacity, 4074);
    //         assert_eq!(new_chunk.postings_count, 3);
    //         assert_eq!(new_chunk.last_doc_id, 424);
    //         assert_eq!(new_chunk.reserved_spot, 1);
    //     }
    //     let chunk = store.get(0);
    //     assert_eq!(chunk.next_chunk, 1);
    //     let new_chunk = store.get_chunk(chunk.next_chunk as usize);
    //     assert_eq!(new_chunk.capacity, 4074);
    //     assert_eq!(new_chunk.postings_count, 3);
    //     assert_eq!(new_chunk.last_doc_id, 424);
    //     assert_eq!(new_chunk.reserved_spot, 1);
    //     assert_eq!(chunk.capacity, 4074);
    //     assert_eq!(chunk.postings_count, 3);
    //     assert_eq!(chunk.last_doc_id, 204);
    // }
}
