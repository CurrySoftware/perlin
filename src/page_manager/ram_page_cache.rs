use std::collections::BTreeMap;
use std::sync::Arc;

use page_manager::{Page, Block, BlockManager, PageStore, PageId, BlockId, PageCache, PAGESIZE};

const CACHESIZE: usize = 1024;

struct RamPageCache<T: PageStore> {
    cache: Vec<(PageId, Arc<Page>)>,
    construction_cache: Vec<(PageId, Page)>,
    store: T,
}

impl<T: PageStore> RamPageCache<T> {
    pub fn new(store: T) -> Self{
        RamPageCache{
            cache: Vec::with_capacity(CACHESIZE),
            construction_cache: Vec::with_capacity(CACHESIZE),
            store: store
        }
    }
}

impl<T: PageStore> BlockManager for RamPageCache<T> {
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
        if let Ok(index) = self.construction_cache
            .binary_search_by_key(&page_id, |&(pid, _)| pid) {
            self.construction_cache[index].1[block_id] = block;
            if block_id == BlockId::last() {
                // Shove it into store
                let (_, page) = self.construction_cache.remove(index);
                self.store.store_reserved(page_id, page);
            }
            return;
        }
        unreachable!();
    }

    fn flush_page(&mut self, page_id: PageId) {
        if let Ok(index) = self.construction_cache
            .binary_search_by_key(&page_id, |&(pid, _)| pid) {
            let (_, page) = self.construction_cache.remove(index);
            self.store.store_reserved(page_id, page);
            return;
        }
        unreachable!();
    }
}

impl<T: PageStore> PageCache for RamPageCache<T> {
    fn delete_page(&mut self, page_id: PageId) {
        self.store.delete_page(page_id);
        if let Ok(index) = self.cache.binary_search_by_key(&page_id, |&(pid, _)| pid) {
            self.cache.remove(index);
        }
    }

    fn get_page(&mut self, page_id: PageId) -> Arc<Page> {

        match self.cache.binary_search_by_key(&page_id, |&(pid, _)| pid) {
            // Page in cache
            Ok(index) => self.cache[index].1.clone(),
            // Page not in cache
            Err(index) => {
                // Get it, arc it
                let page = Arc::new(self.store.get_page(page_id));
                // If cache is not full
                if self.cache.len() < CACHESIZE {
                    // Insert it
                    self.cache.insert(index, (page_id, page.clone()));
                } else {
                    // Otherwise replace it
                    self.cache[index] = (page_id, page.clone());
                }
                page
            }
        }
    }
}
