mod naive_compressor;

use compressor::naive_compressor::NaiveCompressor;
use utils::ring_buffer::RingBuffer;
use index::posting::Posting;
use page_manager::{BLOCKSIZE, Block};

pub trait Compressor {
    fn compress(&mut RingBuffer<Posting>) -> Option<Block>;
    fn force_compress(&mut RingBuffer<Posting>) -> Block;
    fn decompress(Block, &mut RingBuffer<Posting>);
}
