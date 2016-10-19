use std::fmt;

use storage::compression::VByteEncoded;
use index::boolean_index::posting::Listing;

pub const SIZE: usize = 104;

// TODO: Pubs are wrong here. Seriously!
pub struct IndexingChunk {
    pub previous_chunk: u32, // 4
    pub reserved_spot: u32, // 4
    pub next_chunk: u32, // 4
    pub postings_count: u16, // 2
    pub capacity: u16, // 2
    pub last_doc_id: u64, // 8
    pub data: [u8; SIZE], // leaves 4072 bytes on the page for data
}

impl fmt::Debug for IndexingChunk {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        try!(write!(f,
                    "IndexingChunk {}, Previous: {}, Holds {} postings and has {} spare bytes! last_doc_id is {}\n",
                    self.reserved_spot,
                    self.previous_chunk,
                    self.postings_count,
                    self.capacity,
                    self.last_doc_id));
        try!(write!(f, "Data: {:?}", self.data.to_vec()));
        Ok(())
    }
}

impl IndexingChunk {
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
            // HOTSPOT
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


#[cfg(test)]
mod tests {
    use super::*;
    use std::mem;

    #[test]
    fn basic() {
        let mut chunk = IndexingChunk {
            previous_chunk: 0,
            reserved_spot: 0,
            next_chunk: 0,
            last_doc_id: 0,
            postings_count: 0,
            capacity: super::SIZE as u16,
            data: [0; super::SIZE],
        };

        let listing = vec![(0, vec![0, 10, 20]), (20, vec![24, 25, 289]), (204, vec![209, 2456])];
        chunk.append(&listing);
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
        let mut chunk = IndexingChunk {
            previous_chunk: 0,
            reserved_spot: 0,
            next_chunk: 0,
            last_doc_id: 0,
            postings_count: 0,
            capacity: SIZE as u16,
            data: unsafe { mem::uninitialized() },
        };
        assert_eq!(chunk.append(&listing), Ok(()));
        println!("{:?}", chunk);
        assert_eq!(chunk.postings_count, 64);
        assert_eq!(chunk.capacity, 0);
    }

    #[test]
    fn overflowing_single() {
        let listing = vec![(0, (0..10000).collect::<Vec<_>>())];
        let mut chunk = IndexingChunk {
            previous_chunk: 0,
            reserved_spot: 0,
            next_chunk: 0,
            last_doc_id: 0,
            postings_count: 0,
            capacity: SIZE as u16,
            data: unsafe { mem::uninitialized() },
        };
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
        let mut chunk = IndexingChunk {
            previous_chunk: 0,
            reserved_spot: 0,
            next_chunk: 0,
            last_doc_id: 0,
            postings_count: 0,
            capacity: SIZE as u16,
            data: unsafe { mem::uninitialized() },
        };
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
