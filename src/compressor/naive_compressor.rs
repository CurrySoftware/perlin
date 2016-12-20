use std::mem;

use utils::ring_buffer::RingBuffer;
use index::posting::{Posting, DocId};
use page_manager::{BLOCKSIZE, Block};
use compressor::Compressor;


pub struct NaiveCompressor;

impl Compressor for NaiveCompressor {

    fn compress(data: &mut RingBuffer<Posting>) -> Option<Block> {
        if data.count() >= BLOCKSIZE/8 {
            //Enough in there to fill the block
            let mut block = [0u8; BLOCKSIZE];
            for i in 0..BLOCKSIZE/8  {
                block[i*8..(i*8)+8].copy_from_slice(unsafe {&mem::transmute::<Posting, [u8; 8]>(data.pop_front().unwrap())});
            }
            Some(Block(block))               
        } else {
            return None;
        }
    }
    
    fn force_compress(data: &mut RingBuffer<Posting>) -> Block {
        let mut block = [0u8; BLOCKSIZE];
        for i in 0..BLOCKSIZE/8  {
            let posting = data.pop_front().unwrap_or(Posting(DocId::none()));
            block[i*8..(i*8)+8].copy_from_slice(unsafe {&mem::transmute::<Posting, [u8; 8]>(posting)});
        }
        Block(block)        
    }        

    fn decompress(data: Block, target: &mut RingBuffer<Posting>) {
        let nums: [u64; BLOCKSIZE/8] = unsafe{mem::transmute(data)};
        for num in &nums {
            let did = DocId(*num);
            if did != DocId::none() {
                target.push_back(Posting(did));
            } else
            {
                return;
            }
        }
    }

}


#[cfg(test)]
mod tests {
    use utils::ring_buffer::RingBuffer;
    use index::posting::{DocId, Posting};
    use page_manager::{BLOCKSIZE, Block};
    use compressor::Compressor;

    use super::NaiveCompressor;

    #[test]
    fn compress() {
        let mut buffer = RingBuffer::<Posting>::new();
        assert_eq!(NaiveCompressor::compress(&mut buffer), None);
        for i in 0..BLOCKSIZE/8 {
            buffer.push_back(Posting(DocId(i as u64)));
        }
        assert!(NaiveCompressor::compress(&mut buffer).is_some());
        assert_eq!(buffer.count(), 0);
    }
    
    #[test]
    fn decompress() {
        let mut buffer = RingBuffer::<Posting>::new();
        assert_eq!(NaiveCompressor::compress(&mut buffer), None);
        for i in 0..BLOCKSIZE/8 {
            buffer.push_back(Posting(DocId(i as u64)));
        }
        let block = NaiveCompressor::compress(&mut buffer).unwrap();
        assert_eq!(buffer.count(), 0);
        NaiveCompressor::decompress(block, &mut buffer);
        for i in 0..BLOCKSIZE/8 {
            assert_eq!(buffer.pop_front().unwrap(), Posting(DocId(i as u64)));
        }
    }

    #[test]
    fn force_compress() {
        let mut buffer = RingBuffer::<Posting>::new();
        assert_eq!(NaiveCompressor::compress(&mut buffer), None);
        buffer.push_back(Posting(DocId(0)));
        buffer.push_back(Posting(DocId(1)));
        assert_eq!(NaiveCompressor::compress(&mut buffer), None);
        let block = NaiveCompressor::force_compress(&mut buffer);
        assert_eq!(buffer.count(), 0);
        NaiveCompressor::decompress(block, &mut buffer);
        assert_eq!(buffer.pop_front().unwrap(), Posting(DocId(0)));
        assert_eq!(buffer.pop_front().unwrap(), Posting(DocId(1)));
        assert_eq!(buffer.pop_front(), None);
    }
    
}
