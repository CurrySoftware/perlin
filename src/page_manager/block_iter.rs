use std::sync::Arc;

use page_manager::{Pages, Page, PageId, BlockId, Block, RamPageCache, PageCache};

pub struct BlockIter<'a> {
    cache: &'a RamPageCache,
    current_page: Arc<Page>,
    pages: Pages,
    block_counter: BlockId,
    page_counter: usize,
}

impl<'a> BlockIter<'a> {
    pub fn new(cache: &'a RamPageCache, pages: Pages) -> Self {
        BlockIter {
            cache: cache,
            current_page: Arc::new(Page::empty()),
            pages: pages,
            block_counter: BlockId::first(),
            page_counter: 0,
        }
    }

    fn next_page_id(&mut self) -> Option<PageId> {
        self.page_counter += 1;
        if self.page_counter <= self.pages.0.len() {
            let p_id = self.pages.0[self.page_counter - 1];
            Some(p_id)
        } else if let Some(unfull_page) = self.pages.1 {
            self.block_counter = unfull_page.from();
            Some(unfull_page.0)
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
        //Special case for last block:
        //1. Unfull page has to exist
        //2. BlockCounter must be >= unfull_page.to()
        if self.page_counter == self.pages.len() && self.pages.1.is_some() &&
           self.block_counter >= self.pages.1.map(|unfull_page| unfull_page.to()).unwrap() {
            return None;
        }
        let res = Some(self.current_page[self.block_counter]);
        self.block_counter.inc();
        res
    }
}


#[cfg(test)]
mod tests {
    use test_utils::create_test_dir;

    use super::BlockIter;
    use page_manager::{UnfullPage, RamPageCache, BlockManager, FsPageManager, Pages, PageId,
                       Block, BlockId, BLOCKSIZE, PAGESIZE};



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
        let pages = Pages((0..2048).map(|i| PageId(i)).collect::<Vec<_>>(), None);
        let mut iter = BlockIter::new(&cache, pages);
        for i in 0..2048 {
            assert_eq!(iter.next(), Some(Block([(i % 255) as u8; BLOCKSIZE])));
            for j in 1..PAGESIZE {
                assert_eq!(iter.next(), Some(Block([(j % 255) as u8; BLOCKSIZE])));
            }
        }
    }

    #[test]
    fn unfull() {
        let mut cache = new_cache("unfull");
        assert_eq!(cache.store_block(Block([1; BLOCKSIZE])), PageId(0));
        assert_eq!(cache.flush_unfull(PageId(0), BlockId(1)),
                   UnfullPage::new(PageId(0), BlockId(1), BlockId(2)));
        let mut iter =
            BlockIter::new(&cache,
                           Pages(Vec::new(),
                                 Some(UnfullPage::new(PageId(0), BlockId(1), BlockId(2)))));
        assert_eq!(iter.next(), Some(Block([1; BLOCKSIZE])));
        assert_eq!(iter.next(), None);
    }


    #[test]
    fn full_unfull() {
        let mut cache = new_cache("full_unfull");
        // Fill with full pages
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
        // Add Unfull page
        assert_eq!(cache.store_block(Block([1; BLOCKSIZE])), PageId(2048));
        assert_eq!(cache.flush_unfull(PageId(2048), BlockId(1)),
                   UnfullPage::new(PageId(2048), BlockId(1), BlockId(2)));
        let mut iter =
            BlockIter::new(&cache,
                           Pages((0..2048).map(|i| PageId(i)).collect::<Vec<_>>(),
                                 Some(UnfullPage::new(PageId(2048), BlockId(1), BlockId(2)))));
        // Iterate over full pages
        for i in 0..2048 {
            assert_eq!(iter.next(), Some(Block([(i % 255) as u8; BLOCKSIZE])));
            for j in 1..PAGESIZE {
                assert_eq!(iter.next(), Some(Block([(j % 255) as u8; BLOCKSIZE])));
            }
        }
        // Unfull page
        assert_eq!(iter.next(), Some(Block([1; BLOCKSIZE])));
        assert_eq!(iter.next(), None);
    }

    #[test]
    fn filled_unfull() {
        let mut cache = new_cache("filled_unfull");
        assert_eq!(cache.store_block(Block([0; BLOCKSIZE])), PageId(0));
        for i in 1..PAGESIZE - 1 {
            cache.store_in_place(PageId(0),
                                 BlockId(i as u16),
                                 Block([(i % 255) as u8; BLOCKSIZE]));            
        }
        let unfull_page = cache.flush_unfull(PageId(0), BlockId::last());
        let mut iter = BlockIter::new(&cache, Pages(Vec::new(), Some(unfull_page)));
        for i in 0..PAGESIZE -1 {
            assert_eq!(iter.next(), Some(Block([(i % 255) as u8; BLOCKSIZE])));
        }
        assert_eq!(iter.next(), None);
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
        let pages1 = Pages((0..1024).map(|i| PageId(i)).collect::<Vec<_>>(), None);
        let pages2 = Pages((1024..2048).map(|i| PageId(i)).collect::<Vec<_>>(), None);
        let mut iter1 = BlockIter::new(&cache, pages1);
        let mut iter2 = BlockIter::new(&cache, pages2);
        for i in 0..1024 {
            assert_eq!(iter1.next(), Some(Block([(i % 255) as u8; BLOCKSIZE])));
            assert_eq!(iter2.next(),
                       Some(Block([((i + 1024) % 255) as u8; BLOCKSIZE])));
            for j in 1..PAGESIZE {
                assert_eq!(iter1.next(), Some(Block([(j % 255) as u8; BLOCKSIZE])));
                assert_eq!(iter2.next(), Some(Block([(j % 255) as u8; BLOCKSIZE])));
            }
        }
    }

}
