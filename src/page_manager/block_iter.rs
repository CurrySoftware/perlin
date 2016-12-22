use std::mem;

use std::cell::RefCell;
use std::sync::Arc;

use page_manager::{Page, PageId, BlockId, Block, RamPageCache, PageCache};

pub struct BlockIter<'a> {
    cache: &'a RamPageCache,
    current_page: Arc<Page>,
    pages: Vec<PageId>,
    last_block: BlockId,
    block_counter: BlockId,
    page_counter: usize,
}

impl<'a> BlockIter<'a> {
    pub fn new(cache: &'a RamPageCache, pages: Vec<PageId>, last_block: BlockId) -> Self {
        BlockIter {
            cache: cache,
            current_page: Arc::new(Page::empty()),
            pages: pages,
            last_block: last_block,
            block_counter: BlockId::first(),
            page_counter: 0,
        }
    }

    fn next_page_id(&mut self) -> Option<PageId> {
        if self.page_counter < self.pages.len() {
            let p_id = self.pages[self.page_counter];
            self.page_counter += 1;
            Some(p_id)
        } else {
            None
        }
    }
}

impl<'a> Iterator for BlockIter<'a> {
    type Item = Block;

    fn next(&mut self) -> Option<Self::Item> {
        if self.block_counter == BlockId::first() {
            // Get new page
            let page = self.cache.get_page(try_option!(self.next_page_id()));
            self.current_page = page;
        }
        let res = Some(self.current_page[self.block_counter]);
        self.block_counter.inc();
        if self.page_counter == self.pages.len() && self.block_counter > self.last_block {
            return None;
        }
        res
    }
}


#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use test_utils::create_test_dir;

    use super::BlockIter;
    use page_manager::{RamPageCache, BlockManager, FsPageManager, Page, PageCache, PageId, Block,
                       BlockId, BLOCKSIZE, PAGESIZE};



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
            for j in 1..PAGESIZE {
                cache.store_in_place(PageId(i),
                                     BlockId(j as u16),
                                     Block([(j % 255) as u8; BLOCKSIZE]));
            }
            cache.flush_page(PageId(i));
        }
        let pages = (0..2048).map(|i| PageId(i)).collect::<Vec<_>>();
        let mut iter = BlockIter::new(&cache, pages, BlockId::last());
        for i in 0..2048 {
            assert_eq!(iter.next(), Some(Block([(i % 255) as u8; BLOCKSIZE])));
            for j in 1..PAGESIZE {
                assert_eq!(iter.next(), Some(Block([(j % 255) as u8; BLOCKSIZE])));
            }
        }
    }

    #[test]
    fn multiple_readers() {
        let mut cache = new_cache("basic");
        for i in 0..2048 {
            assert_eq!(cache.store_block(Block([(i % 255) as u8; BLOCKSIZE])),
                       PageId(i));
            for j in 1..PAGESIZE {
                cache.store_in_place(PageId(i),
                                     BlockId(j as u16),
                                     Block([(j % 255) as u8; BLOCKSIZE]));
            }
            cache.flush_page(PageId(i));
        }
        let pages1 = (0..1024).map(|i| PageId(i)).collect::<Vec<_>>();
        let pages2 = (1024..2048).map(|i| PageId(i)).collect::<Vec<_>>();
        let mut iter1 = BlockIter::new(&cache, pages1, BlockId::last());
        let mut iter2 = BlockIter::new(&cache, pages2, BlockId::last());
        for i in 0..1024 {
            assert_eq!(iter1.next(), Some(Block([(i % 255) as u8; BLOCKSIZE])));
            assert_eq!(iter2.next(), Some(Block([((i + 1024) % 255) as u8; BLOCKSIZE])));
            for j in 1..PAGESIZE {
                assert_eq!(iter1.next(), Some(Block([(j % 255) as u8; BLOCKSIZE])));
                assert_eq!(iter2.next(), Some(Block([(j % 255) as u8; BLOCKSIZE])));
            }
        }
    }

}
