use compressor::Compressor;
use page_manager::BlockIter;
use utils::ring_buffer::BiasedRingBuffer;
use utils::Baseable;
use index::listing::UsedCompressor;


#[derive(Debug, PartialEq, Copy, Clone)]
pub struct Posting(pub DocId);
#[derive(Debug, PartialEq, Copy, Clone)]
pub struct DocId(pub u64);

impl DocId {
    #[inline]
    pub fn none() -> DocId {
        DocId(u64::max_value())
    }
}

impl<'a> Baseable<&'a DocId> for DocId {
    #[inline]
    fn base_on(&mut self, other: &Self) {
        self.0 -= other.0
    }
}

impl Posting {
    #[inline]
    pub fn none() -> Posting {
        Posting(DocId::none())
    }
}

impl Default for Posting {
    fn default() -> Self {
        Posting(DocId(0))
    }
}

impl<'a> Baseable<&'a Posting> for Posting {
    #[inline]
    fn base_on(&mut self, other: &Self){
        self.0.base_on(&other.0);
    }
}


pub struct PostingIterator<'a> {
    blocks: BlockIter<'a>,
    posting_buffer: BiasedRingBuffer<Posting>
}

impl<'a> PostingIterator<'a> {
    pub fn new(blocks: BlockIter<'a>) -> Self {
        PostingIterator {
            blocks: blocks,
            posting_buffer: BiasedRingBuffer::new()
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
