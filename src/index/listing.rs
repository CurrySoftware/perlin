use utils::ring_buffer::RingBuffer;

use compressor::{Compressor, NaiveCompressor};

use page_manager::{PageId, Block, BlockId, RamPageCache, BlockManager};

use index::posting::Posting;

type UsedCompressor = NaiveCompressor;

pub struct Listing {
    block_list: Vec<PageId>,
    last_block_id: BlockId,
    posting_buffer: RingBuffer<Posting>,
}


impl Listing {
    pub fn add(&mut self, postings: &[Posting], page_cache: &mut RamPageCache) {
        for posting in postings {
            self.posting_buffer.push_back(*posting);
        }
        self.compress_and_ship(page_cache, false);
    }

    pub fn commit(&mut self, page_cache: &mut RamPageCache) {
        self.compress_and_ship(page_cache, true);
        page_cache.flush_page(*self.block_list.last().unwrap());
    }

    fn compress_and_ship(&mut self, page_cache: &mut RamPageCache, force: bool) {
        while let Some(block) = UsedCompressor::compress(&mut self.posting_buffer) {
            self.ship(page_cache, block);
        }
        if force && self.posting_buffer.count() > 0 {
            let block = UsedCompressor::force_compress(&mut self.posting_buffer);
            self.ship(page_cache, block);
        }
    }

    fn ship(&mut self, page_cache: &mut RamPageCache, block: Block) {
        if self.last_block_id == BlockId::last() {
            self.block_list.push(page_cache.store_block(block));
            self.last_block_id = BlockId::first();
        } else {
            self.last_block_id.inc();
            page_cache.store_in_place(*self.block_list.last().unwrap(), self.last_block_id, block)
        }
    }
}


#[cfg(test)]
mod tests {


}
