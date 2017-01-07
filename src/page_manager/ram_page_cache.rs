use std::sync::{RwLock, Arc};

use utils::counter::Counter;
use page_manager::{FsPageManager, UnfullPage, Page, Block, BlockManager, PageStore, PageId,
                   BlockId, PageCache};

const CACHESIZE: usize = 1024;

pub struct RamPageCache {
    cache: RwLock<Vec<(PageId, Arc<Page>)>>,
    counter: Counter,
    construction_cache: Vec<(PageId, Page)>,
    store: FsPageManager,
}

impl RamPageCache {
    pub fn new(store: FsPageManager) -> Self {
        RamPageCache {
            counter: Counter::new(),
            cache: RwLock::new(Vec::with_capacity(CACHESIZE)),
            construction_cache: Vec::with_capacity(CACHESIZE),
            store: store,
        }
    }

    #[inline]
    fn search_page(&self, page_id: &PageId) -> Result<usize, usize> {
        self.cache.read().unwrap().binary_search_by_key(page_id, |&(pid, _)| pid)
    }

    fn invalidate(&mut self, page_id: PageId) {
        if let Ok(index) = self.search_page(&page_id) {
            self.cache.write().unwrap().remove(index);
        }
    }
}

impl BlockManager for RamPageCache {
    fn store_block(&mut self, block: Block) -> PageId {
        let page_id = PageId(self.counter.retrieve_and_inc());
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
            // Page is currently beeing built
            Ok(index) => {
                self.construction_cache[index].1[block_id] = block;
            }
            // Page was already flushed. Retrieve it, change it
            Err(index) => {
                let mut page = self.store.get_page(page_id);
                page[block_id] = block;
                self.construction_cache.insert(index, (page_id, page));
            }
        }
    }

    fn flush_page(&mut self, page_id: PageId) -> PageId {
        if let Ok(index) = self.construction_cache
            .binary_search_by_key(&page_id, |&(pid, _)| pid) {
            let (_, page) = self.construction_cache.remove(index);
            self.invalidate(page_id);
            return self.store.store_full(page);
        }
        unreachable!();
        // If page is not in cache it needs not to be flushed
    }

    fn flush_unfull(&mut self, page_id: PageId, block_id: BlockId) -> UnfullPage {
        if let Ok(index) = self.construction_cache
            .binary_search_by_key(&page_id, |&(pid, _)| pid) {
            let (_, page) = self.construction_cache.remove(index);
            self.invalidate(page_id);
            return self.store.store_unfull(page, block_id);
        }
        unreachable!();
        // If page is not in cache it needs not to be flushed
    }
}

impl PageCache for RamPageCache {
    fn delete_page(&mut self, page_id: PageId) {
        self.store.delete_page(page_id);
        self.invalidate(page_id);
    }

    fn delete_unfull(&mut self, page_id: PageId) {
        self.store.delete_unfull(page_id);
        self.invalidate(page_id);
    }
    
    fn get_page(&self, page_id: PageId) -> Arc<Page> {
        use std::cmp;
        match self.search_page(&page_id) {
            // Page in cache
            Ok(index) => self.cache.read().unwrap()[index].1.clone(),
            // Page not in cache
            Err(index) => {
                // Get it, arc it
                let page = Arc::new(self.store.get_page(page_id));
                // If cache is not full
                if self.cache.read().unwrap().len() < CACHESIZE {
                    // Insert it
                    self.cache.write().unwrap().insert(index, (page_id, page.clone()));
                } else {
                    // Otherwise replace it
                    let index = cmp::min(index, CACHESIZE - 1);
                    self.cache.write().unwrap()[index] = (page_id, page.clone());
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
    use page_manager::{BlockManager, FsPageManager, Page, PageCache, UnfullPage, PageId, Block,
                       BlockId, BLOCKSIZE, PAGESIZE};


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
        cache.flush_page(PageId(0));
        cache.flush_page(PageId(1));
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
    fn basic_unfull() {
        let mut pmgr = new_cache("basic_unfull");
        assert_eq!(pmgr.store_block(Block([1; BLOCKSIZE])), PageId(0));
        assert_eq!(pmgr.flush_unfull(PageId(0), BlockId(1)),
                   UnfullPage::new(PageId(0), BlockId(1), BlockId(2)));
        let mut p = Page::empty();
        p[BlockId::first()].0[0] = 1;
        p[BlockId(1)] = Block([1; BLOCKSIZE]);
        assert_eq!(pmgr.get_page(PageId(0)), Arc::new(p));
    }


    #[test]
    fn flush_full() {
        let mut cache = new_cache("flush_full");
        assert_eq!(cache.store_block(Block([0; BLOCKSIZE])), PageId(0));
        let mut ref_page = Page::empty();
        for j in 1..PAGESIZE {
            cache.store_in_place(PageId(0),
                                 BlockId(j as u16),
                                 Block([(j % 255) as u8; BLOCKSIZE]));
            ref_page[BlockId(j as u16)] = Block([(j % 255) as u8; BLOCKSIZE]);
        }
        cache.flush_page(PageId(0));
        assert_eq!(cache.get_page(PageId(0)), Arc::new(ref_page));
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
}
