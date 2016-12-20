use std::cell::RefCell;
use std::sync::Arc;

use page_manager::{FsPageManager, Page, Block, BlockManager, PageStore, PageId, BlockId, PageCache};

const CACHESIZE: usize = 1024;

pub struct RamPageCache {
    cache: RefCell<Vec<(PageId, Arc<Page>)>>,
    construction_cache: Vec<(PageId, Page)>,
    store: FsPageManager,
}

impl RamPageCache {
    pub fn new(store: FsPageManager) -> Self {
        RamPageCache {
            cache: RefCell::new(Vec::with_capacity(CACHESIZE)),
            construction_cache: Vec::with_capacity(CACHESIZE),
            store: store,
        }
    }

    #[inline]
    fn search_page(&self, page_id: &PageId) -> Result<usize, usize>  {
        self.cache.borrow().binary_search_by_key(page_id, |&(pid, _)| pid)
    }

    fn invalidate(&mut self, page_id: PageId) {
        if let Ok(index) = self.search_page(&page_id) {
            self.cache.borrow_mut().remove(index);
        }
    }
}

impl BlockManager for RamPageCache {
    fn store_block(&mut self, block: Block) -> PageId {
        let page_id = self.store.reserve_page();
        let mut p = Page::empty();
        p[BlockId::first()] = block;
        if let Err(index) = self.construction_cache
            .binary_search_by_key(&page_id, |&(pid, _)| pid) {
            self.construction_cache.insert(index, (page_id, p));
            return page_id;
        }
        unreachable!();
    }

    fn store_in_place(&mut self, page_id: PageId, block_id: BlockId, block: Block) {
        match self.construction_cache
            .binary_search_by_key(&page_id, |&(pid, _)| pid) {
            Ok(index) => {
                self.construction_cache[index].1[block_id] = block;
                if block_id == BlockId::last() {
                    // Shove it into store
                    let (_, page) = self.construction_cache.remove(index);
                    self.store.store_reserved(page_id, page);
                }
                return;
            }
            Err(index) => {
                let mut page = self.store.get_page(page_id);
                page[block_id] = block;
                self.construction_cache.insert(index, (page_id, page));
            }
        }
    }

    fn flush_page(&mut self, page_id: PageId) {
        if let Ok(index) = self.construction_cache
            .binary_search_by_key(&page_id, |&(pid, _)| pid) {
            let (_, page) = self.construction_cache.remove(index);
            self.invalidate(page_id);
            self.store.store_reserved(page_id, page);
            return;
        }
        unreachable!();
    }
}

impl PageCache for RamPageCache {
    fn delete_page(&mut self, page_id: PageId) {
        self.store.delete_page(page_id);
        self.invalidate(page_id);
    }

    fn get_page(&self, page_id: PageId) -> Arc<Page> {
        use std::cmp;
        match self.search_page(&page_id) {
            // Page in cache
            Ok(index) => self.cache.borrow()[index].1.clone(),
            // Page not in cache
            Err(index) => {
                // Get it, arc it
                let page = Arc::new(self.store.get_page(page_id));
                // If cache is not full
                if self.cache.borrow().len() < CACHESIZE {
                    // Insert it
                    self.cache.borrow_mut().insert(index, (page_id, page.clone()));
                } else {
                    // Otherwise replace it
                    let index = cmp::min(index, CACHESIZE - 1);
                    self.cache.borrow_mut()[index] = (page_id, page.clone());
                }
                page
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use test_utils::create_test_dir;

    use super::RamPageCache;
    use page_manager::{BlockManager, FsPageManager, Page, PageCache, PageId, Block, BlockId,
                       BLOCKSIZE};


    fn new_cache(name: &str) -> RamPageCache {
        let path = &create_test_dir(format!("ram_page_cache/{}", name).as_str());
        let pmgr = FsPageManager::new(&path.join("pages.bin"));
        RamPageCache::new(pmgr)
    }

    #[test]
    fn basic() {
        // New Cache
        let mut cache = new_cache("basic");
        assert_eq!(cache.store_block(Block([1; BLOCKSIZE])), PageId(0));
        assert_eq!(cache.store_block(Block([2; BLOCKSIZE])), PageId(1));
        assert_eq!(cache.store_block(Block([3; BLOCKSIZE])), PageId(2));
        cache.store_in_place(PageId(0), BlockId(1), Block([15; BLOCKSIZE]));
        cache.flush_page(PageId(1));
        cache.flush_page(PageId(0));
        cache.flush_page(PageId(2));
        let mut p0 = Page::empty();
        p0[BlockId::first()] = Block([1; BLOCKSIZE]);
        p0[BlockId(1)] = Block([15; BLOCKSIZE]);
        assert_eq!(cache.get_page(PageId(0)), Arc::new(p0));
        let mut p2 = Page::empty();
        p2[BlockId::first()] = Block([3; BLOCKSIZE]);
        assert_eq!(cache.get_page(PageId(2)), Arc::new(p2));
    }

    #[test]
    fn extended() {
        let mut cache = new_cache("extended");
        for i in 0..2048 {
            assert_eq!(cache.store_block(Block([(i % 255) as u8; BLOCKSIZE])),
                       PageId(i));
            cache.flush_page(PageId(i));
        }
        for i in 0..2048 {
            let mut p = Page::empty();
            p[BlockId::first()] = Block([(i % 255) as u8; BLOCKSIZE]);
            assert_eq!(cache.get_page(PageId(i)), Arc::new(p));
        }
    }

    #[test]
    fn mutation() {
        let mut cache = new_cache("mutation");
        for i in 0..2048 {
            assert_eq!(cache.store_block(Block([(i % 255) as u8; BLOCKSIZE])),
                       PageId(i));
            cache.flush_page(PageId(i));
        }
        for i in 0..2048 {
            let mut p = Page::empty();
            p[BlockId::first()] = Block([(i % 255) as u8; BLOCKSIZE]);
            assert_eq!(cache.get_page(PageId(i)), Arc::new(p));
        }
        for i in 0..2048 {
            cache.store_in_place(PageId(i),
                                 BlockId(1),
                                 Block([((i + 1) % 255) as u8; BLOCKSIZE]));
            cache.flush_page(PageId(i));
        }
        for i in 0..2048 {
            let mut p = Page::empty();
            p[BlockId::first()] = Block([(i % 255) as u8; BLOCKSIZE]);
            p[BlockId(1)] = Block([((i + 1) % 255) as u8; BLOCKSIZE]);
            assert_eq!(cache.get_page(PageId(i)), Arc::new(p));
        }
    }

    #[test]
    fn read_during_mutation() {
        let mut cache = new_cache("read_during_mutation");
        for i in 0..2048 {
            assert_eq!(cache.store_block(Block([(i % 255) as u8; BLOCKSIZE])),
                       PageId(i));
            cache.flush_page(PageId(i));
        }
        for i in 0..2048 {
            let mut p = Page::empty();
            p[BlockId::first()] = Block([(i % 255) as u8; BLOCKSIZE]);
            assert_eq!(cache.get_page(PageId(i)), Arc::new(p));
        }
        for i in 0..2048 {
            cache.store_in_place(PageId(i),
                                 BlockId(1),
                                 Block([((i + 1) % 255) as u8; BLOCKSIZE]));
        }
        for i in 0..2048 {
            let mut p = Page::empty();
            p[BlockId::first()] = Block([(i % 255) as u8; BLOCKSIZE]);
            assert_eq!(cache.get_page(PageId(i)), Arc::new(p));
        }
        for i in 0..2048 {
            cache.flush_page(PageId(i));
        }
        for i in 0..2048 {
            let mut p = Page::empty();
            p[BlockId::first()] = Block([(i % 255) as u8; BLOCKSIZE]);
            p[BlockId(1)] = Block([((i + 1) % 255) as u8; BLOCKSIZE]);
            assert_eq!(cache.get_page(PageId(i)), Arc::new(p));
        }
    }
}
