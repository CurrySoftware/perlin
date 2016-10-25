use std::mem;
use std::fmt;

use storage::compression::VByteEncoded;
use index::boolean_index::posting::{decode_from_chunk, Listing};

pub const SIZE: usize = 104;


pub struct IndexingChunk {
    previous_chunk: u32, // 4
    postings_count: u16, // 2
    capacity: u16, // 2
    last_doc_id: u64, // 8
    data: [u8; SIZE], // leaves 4072 bytes on the page for data
}

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

    /// Adds listing to IndexingChunk. Returns Ok if listing fits into chunk
    /// Otherwise returns the posting number which did not fit into this chunk anymore
    pub fn append(&mut self, listing: &[(u64, Vec<u32>)]) -> Result<(), usize> {
        let mut working_slice = &mut self.data[SIZE - self.capacity as usize..];
        for (count, &(doc_id, ref positions)) in listing.iter().enumerate() {
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
                        return Err(count);
                    }
                }
            } else {
                self.capacity = old_capa;
                self.last_doc_id = old_doc_id;
                return Err(count);
            }
        }
        Ok(())
    }
}

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
            self.hot_chunks[id as usize] = IndexingChunk::new(0, 0);
            return &mut self.hot_chunks[id as usize];
        }
        self.hot_chunks.push(IndexingChunk::new(0, 0));
        &mut self.hot_chunks[id as usize]
    }

    pub fn next_chunk(&mut self, id: u64) -> &mut IndexingChunk {
        let next = IndexingChunk::new(self.archived_chunks.len() as u32 + 1,
                                      self.hot_chunks[id as usize].last_doc_id);
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
                    Ok(mut new) => {
                        new.append(&mut listing);
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
        }
        let chunk = store.get_current(0);
        let new_chunk = store.get_archived(chunk.previous_chunk as usize);
        assert_eq!(new_chunk.capacity, 4074);
        assert_eq!(new_chunk.postings_count, 3);
        assert_eq!(new_chunk.last_doc_id, 424);
        assert_eq!(chunk.capacity, 4074);
        assert_eq!(chunk.postings_count, 3);
        assert_eq!(chunk.last_doc_id, 204);
    }

    #[test]
    fn chunk_basic() {
        let mut chunk = IndexingChunk::new(0, 0);

        let listing = vec![(0, vec![0, 10, 20]), (20, vec![24, 25, 289]), (204, vec![209, 2456])];
        chunk.append(&listing).unwrap();
        println!("{:?}", chunk);
        assert_eq!(chunk.capacity, 4054);
        assert_eq!(chunk.postings_count, 3);
        assert_eq!(chunk.last_doc_id, 204);
    }

    #[test]
    fn full() {
        let positions = (0..61).collect::<Vec<_>>();
        let mut listing = (0..64).map(|i| (i, positions.clone())).collect::<Vec<_>>();
        let mut additional = (120..160).collect::<Vec<_>>();
        listing[0].1.append(&mut additional);
        let mut chunk = IndexingChunk::new(0, 0);
        assert_eq!(chunk.append(&listing), Ok(()));
        println!("{:?}", chunk);
        assert_eq!(chunk.postings_count, 64);
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
        let positions = (0..61).collect::<Vec<_>>();
        let mut listing = (0..64).map(|i| (i, positions.clone())).collect::<Vec<_>>();
        let mut additional = (120..150).collect::<Vec<_>>();
        listing[0].1.append(&mut additional);
        let mut chunk = IndexingChunk::new(0, 0);
        assert_eq!(chunk.append(&listing), Ok(()));
        assert_eq!(chunk.postings_count, 64);
        assert_eq!(chunk.capacity, 10);
        assert_eq!(chunk.last_doc_id, 63);
        let listing = vec![(64, vec![0]), (65, (0..10000).collect::<Vec<_>>())];
        assert_eq!(chunk.append(&listing), Err(1));
        assert_eq!(chunk.postings_count, 65);
        assert_eq!(chunk.capacity, 7);
        assert_eq!(chunk.last_doc_id, 64);
    }
}
