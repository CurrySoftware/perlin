mod naive_compressor;

pub use compressor::naive_compressor::NaiveCompressor;
use utils::ring_buffer::BiasedRingBuffer;
use index::posting::Posting;
use page_manager::Block;

pub trait Compressor {
    fn compress(&mut BiasedRingBuffer<Posting>) -> Option<Block>;
    fn force_compress(&mut BiasedRingBuffer<Posting>) -> Block;
    fn decompress(Block, &mut BiasedRingBuffer<Posting>);
}
