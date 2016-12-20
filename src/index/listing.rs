use utils::ring_buffer::RingBuffer;

use page_manager::{PageId, BlockId};

use index::posting::Posting;

pub struct Listing {
    block_list: Vec<PageId>,
    last_block: BlockId,
    posting_buffer: RingBuffer<Posting>
}


impl Listing {

    fn add(&mut self, postings: &[Posting]){

    } 

}
