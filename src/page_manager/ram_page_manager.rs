use std::sync::Arc;
use page_manager::{Page, Block, PageManager, BlockManager, PageId, BlockId, PageCache, PAGESIZE};


struct RamPageManager {
    pages: Vec<(Arc<Page>, usize)>,
    unpopulated_pages: Vec<usize>,
}

impl RamPageManager {
    fn new() -> Self {
        RamPageManager {
            pages: Vec::new(),
            unpopulated_pages: Vec::new(),
        }
    }

    fn store_page_capacity(&mut self, page: Page, capacity: usize) -> PageId {
        if self.unpopulated_pages.is_empty() {
            self.pages.push((Arc::new(page), capacity));
            PageId((self.pages.len() - 1) as u64)
        } else {
            let id = self.unpopulated_pages.swap_remove(0);
            self.pages[id] = (Arc::new(page), capacity);
            PageId(id as u64)
        }
    }
}

impl PageManager for RamPageManager {
    #[inline]
    fn store_page(&mut self, page: Page) -> PageId {
        self.store_page_capacity(page, 0)
    }

    fn delete_page(&mut self, page_id: PageId) {
        let id = page_id.0 as usize;
        self.unpopulated_pages.push(id);
    }
}

impl PageCache for RamPageManager {
    fn get_page(&self, page_id: PageId) -> Arc<Page> {
        let id = page_id.0 as usize;
        self.pages[id].0.clone()
    }
}

impl BlockManager for RamPageManager {
    fn store_block(&mut self, block: Block) -> PageId {
        let mut p = Page::empty();
        p[BlockId(0)] = block;
        self.store_page_capacity(p, PAGESIZE - 1)
    }

    fn store_in_page(&mut self, page_id: PageId, block: Block) -> Result<BlockId, PageId> {
        let id = page_id.0 as usize;
        {
            let (ref mut page, ref mut capa) = self.pages[id];
            if *capa > 0 {
                let block_id = BlockId::from_page_capacity(*capa);
                Arc::make_mut(page)[block_id] = block;
                *capa -= 1;
                return Ok(block_id);
            }
        }
        Err(self.store_block(block))
    }
}


#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use super::RamPageManager;

    use page_manager::{Page, PageManager, PageCache, PageId, Block, BlockId, BLOCKSIZE,
                       BlockManager};

    #[test]
    fn store_page() {
        let mut pmgr = RamPageManager::new();
        assert_eq!(pmgr.store_page(Page::empty()), PageId(0));
    }

    #[test]
    fn get_page() {
        let mut pmgr = RamPageManager::new();
        assert_eq!(pmgr.store_page(Page::empty()), PageId(0));
        assert_eq!(pmgr.get_page(PageId(0)), Arc::new(Page::empty()));
    }

    #[test]
    fn delete_page() {
        let mut pmgr = RamPageManager::new();
        assert_eq!(pmgr.store_page(Page::empty()), PageId(0));
        assert_eq!(pmgr.store_page(Page::empty()), PageId(1));
        assert_eq!(pmgr.store_page(Page::empty()), PageId(2));
        assert_eq!(pmgr.store_page(Page::empty()), PageId(3));
        pmgr.delete_page(PageId(1));
        assert_eq!(pmgr.store_page(Page::empty()), PageId(1));
    }

    #[test]
    fn store_block() {
        let mut pmgr = RamPageManager::new();
        assert_eq!(pmgr.store_block(Block([1; BLOCKSIZE])), PageId(0));
        assert_eq!(pmgr.store_in_page(PageId(0), Block([2; BLOCKSIZE])),
                   Ok(BlockId(1)));
        assert_eq!(pmgr.store_in_page(PageId(0), Block([3; BLOCKSIZE])),
                   Ok(BlockId(2)));
    }

    #[test]
    fn get_stored_block() {
        let mut pmgr = RamPageManager::new();
        assert_eq!(pmgr.store_block(Block([1; BLOCKSIZE])), PageId(0));
        let mut p = Page::empty();
        p[BlockId::first()] = Block([1; BLOCKSIZE]);
        assert_eq!(pmgr.get_page(PageId(0)), Arc::new(p));
        assert_eq!(pmgr.store_in_page(PageId(0), Block([2; BLOCKSIZE])), Ok(BlockId(1)));
        p[BlockId(1)] = Block([2; BLOCKSIZE]);
        assert_eq!(pmgr.get_page(PageId(0)), Arc::new(p));
    }

    #[test]
    fn change_while_read() {
        let mut pmgr = RamPageManager::new();
        assert_eq!(pmgr.store_block(Block([1; BLOCKSIZE])), PageId(0));
        let mut p = Page::empty();
        p[BlockId::first()] = Block([1; BLOCKSIZE]);
        assert_eq!(pmgr.get_page(PageId(0)), Arc::new(p));
        let holder = pmgr.get_page(PageId(0));
        assert_eq!(pmgr.store_in_page(PageId(0), Block([2; BLOCKSIZE])), Ok(BlockId(1)));
        p[BlockId(1)] = Block([2; BLOCKSIZE]);
        assert_eq!(pmgr.get_page(PageId(0)), Arc::new(p));
        assert!(holder != Arc::new(p));
    }
}
