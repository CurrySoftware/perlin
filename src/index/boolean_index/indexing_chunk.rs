use std::fmt;

use storage::compression::VByteEncoded;
use index::boolean_index::posting::Listing;

pub struct IndexingChunk {
    previous_chunk: u32,
    reserved_spot: u32,
    last_doc_id: u64,
    postings_count: u16,
    capacity: u16,
    data: [u8; 4092],
}

impl fmt::Debug for IndexingChunk {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f,
               "IndexingChunk {}, Previous: {}, Holds {} postings and has {} spare bytes! last_doc_id is {}. Data:\n \
                {:?}",
               self.reserved_spot,
               self.previous_chunk,
               self.postings_count,
               self.capacity,
               self.last_doc_id,
               self.data.to_vec())
    }
}

impl IndexingChunk {
    /// Adds listing to IndexingChunk. Returns Ok if listing fits into chunk
    /// Otherwise returns the posting number which did not fit into this chunk anymore
    fn add_listing(&mut self, listing: &Listing) -> Result<(), usize> {
        for (count, &(doc_id, ref positions)) in listing.iter().enumerate() {
            let old_capa = self.capacity;
            // Encode the doc_id as delta
            let delta_doc_id = doc_id - self.last_doc_id;
            self.last_doc_id = doc_id;
            // Encode the delta_doc_id with vbyte code
            let encoded_ddoc_id = VByteEncoded::new(delta_doc_id as usize);
            if let Ok(bytes_written) = encoded_ddoc_id.write_to(&mut self.data[(4092 - self.capacity) as usize..]) {
                // There was enough space. Count down capacity. Start encoding and writing positions
                self.capacity -= bytes_written as u16;
                // Encode positions len and add to data
                if let Ok(bytes_written) = VByteEncoded::new(positions.len())
                    .write_to(&mut self.data[(4092 - self.capacity) as usize..]) {
                    self.capacity -= bytes_written as u16;
                } else {
                    self.capacity = old_capa;
                    return Err(count);
                }
                // Encode positions and add to data
                let mut last_position = 0;
                for position in positions {
                    if let Ok(bytes_written) = VByteEncoded::new(*position as usize - last_position)
                        .write_to(&mut self.data[(4092 - self.capacity) as usize..]) {
                        self.capacity -= bytes_written as u16;
                        last_position = *position as usize;
                    } else {
                        self.capacity = old_capa;
                        return Err(count);
                    }
                }
            } else {
                self.capacity = old_capa;
                return Err(count);
            }
            self.postings_count += 1;
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
            last_doc_id: 0,
            postings_count: 0,
            capacity: 4092,
            data: [0; 4092],
        };

        let listing = vec![(0, vec![0, 10, 20]), (20, vec![24, 25, 289]), (204, vec![209, 2456])];
        chunk.add_listing(&listing);
        assert_eq!(chunk.capacity, 4074);
        assert_eq!(chunk.postings_count, 3);
        assert_eq!(chunk.last_doc_id, 204);
    }

    #[test]
    fn full() {
        let positions = (0..61).collect::<Vec<_>>();
        let mut listing = (0..64).map(|i| (i, positions.clone())).collect::<Vec<_>>();
        let mut additional = (120..180).collect::<Vec<_>>();
        listing[0].1.append(&mut additional);
        let mut chunk = IndexingChunk {
            previous_chunk: 0,
            reserved_spot: 0,
            last_doc_id: 0,
            postings_count: 0,
            capacity: 4092,
            data: unsafe { mem::uninitialized() },
        };
        assert_eq!(chunk.add_listing(&listing), Ok(()));
        assert_eq!(chunk.postings_count, 64);
        assert_eq!(chunk.capacity, 0);
    }

    #[test]
    fn overflowing_single() {
        let listing = vec![(0, (0..10000).collect::<Vec<_>>())];
        let mut chunk = IndexingChunk {
            previous_chunk: 0,
            reserved_spot: 0,
            last_doc_id: 0,
            postings_count: 0,
            capacity: 4092,
            data: unsafe { mem::uninitialized() },
        };
        assert_eq!(chunk.add_listing(&listing), Err(0));
        assert_eq!(chunk.capacity, 4092);
        assert_eq!(chunk.postings_count, 0);
    }


}
