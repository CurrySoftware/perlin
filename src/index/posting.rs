use compressor::Compressor;
use page_manager::BlockIter;
use utils::ring_buffer::RingBuffer;
use index::listing::UsedCompressor;


#[derive(Debug, PartialEq, Copy, Clone)]
pub struct Posting(pub DocId);
#[derive(Debug, PartialEq, Copy, Clone)]
pub struct DocId(pub u64);

impl DocId {
    pub fn none() -> DocId {
        DocId(u64::max_value())
    }
}

struct PostingIterator<'a> {
    blocks: BlockIter<'a>,
    posting_buffer: RingBuffer<Posting>
}

impl<'a> PostingIterator<'a> {
    pub fn new(blocks: BlockIter<'a>) -> Self {
        PostingIterator {
            blocks: blocks,
            posting_buffer: RingBuffer::new()
        }
    }
}

impl<'a> Iterator for PostingIterator<'a> {

    type Item = Posting;

    fn next(&mut self) -> Option<Posting> {
        if self.posting_buffer.is_empty() {
            if let Some(block) = self.blocks.next() {
                UsedCompressor::decompress(block, &mut self.posting_buffer);
            }
        }
        self.posting_buffer.pop_back()
    }
}
