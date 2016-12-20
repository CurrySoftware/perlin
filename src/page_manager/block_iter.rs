use std::cell::RefCell;
use std::sync::Arc;

use page_manager::{Page, PageId, BlockId, Block, RamPageCache, PageCache};

pub struct BlockIter<'a> {
    cache: &'a RefCell<RamPageCache>,
    current_page: (PageId, Arc<Page>),
    blocks: Vec<(PageId, BlockId)>,
    ptr: usize,
}

impl<'a> BlockIter<'a> {

    pub fn get_page(&self, page_id: PageId) -> Arc<Page> {
        self.cache.borrow_mut().get_page(page_id)
    }
    
    pub fn new(cache: &'a RefCell<RamPageCache>, blocks: Vec<(PageId, BlockId)>) -> Self {
        let p_id = blocks[0].0;
        let curr_page = (p_id, cache.borrow_mut().get_page(p_id));
        BlockIter {
            cache: cache,
            current_page: curr_page,
            blocks: blocks,
            ptr: 0,
        }
    }
}

impl<'a> Iterator for BlockIter<'a> {
    type Item = Block;

    fn next(&mut self) -> Option<Self::Item> {
        if self.ptr < self.blocks.len() {
            let (page_id, block_id) = self.blocks[self.ptr];
            if self.current_page.0 != page_id {
                self.current_page = (page_id, self.get_page(page_id));
            }
            self.ptr += 1;
            return Some(self.current_page.1[block_id]);
        }
        return None;
    }
}


#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::cell::RefCell;
    use test_utils::create_test_dir;

    use super::BlockIter;
    use page_manager::{RamPageCache, BlockManager, FsPageManager, Page, PageCache, PageId, Block,
                       BlockId, BLOCKSIZE};



    fn new_cache(name: &str) -> RamPageCache {
        let path = &create_test_dir(format!("block_iter/{}", name).as_str());
        let pmgr = FsPageManager::new(&path.join("pages.bin"));
        RamPageCache::new(pmgr)
    }

    #[test]
    fn basic() {
        let mut cache = new_cache("basic");
        for i in 0..2048 {
            assert_eq!(cache.store_block(Block([(i % 255) as u8; BLOCKSIZE])),
                       PageId(i));
            cache.flush_page(PageId(i));
        }
        let blocks = (0..2048).map(|i| (PageId(i), BlockId::first())).collect::<Vec<_>>();
        let rcache = RefCell::new(cache);
        let mut iter = BlockIter::new(&rcache, blocks);
        for i in 0..2048 {
            assert_eq!(iter.next(), Some(Block([(i % 255) as u8; BLOCKSIZE])));
        }
    }

    #[test]
    fn multiple_readers() {
        let mut cache = new_cache("multiple_readers");
        for i in 0..2048 {
            assert_eq!(cache.store_block(Block([(i % 255) as u8; BLOCKSIZE])),
                       PageId(i));
            cache.store_in_place(PageId(i),
                                 BlockId(1),
                                 Block([((i + 1) % 255) as u8; BLOCKSIZE]));
            cache.flush_page(PageId(i));
        }
        let blocks1 = (0..2048).map(|i| (PageId(i), BlockId::first())).collect::<Vec<_>>();
        let blocks2 = (0..2048).map(|i| (PageId(i), BlockId(1))).collect::<Vec<_>>();
        let rcache = RefCell::new(cache);
        let mut iter1 = BlockIter::new(&rcache, blocks1);
        let mut iter2 = BlockIter::new(&rcache, blocks2);
        for i in 0..2048 {
            assert_eq!(iter1.next(), Some(Block([(i % 255) as u8; BLOCKSIZE])));
            assert_eq!(iter2.next(), Some(Block([((i+1) % 255) as u8; BLOCKSIZE])));
        }
    }

}
