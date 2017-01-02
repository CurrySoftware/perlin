use std::mem;

use std::path::Path;
use std::io::{Seek, SeekFrom, Write};
use std::fs::{OpenOptions, File};

use utils::counter::Counter;
use page_manager::{Pages, UnfullPage, Page, PageId, BlockId, PageStore, PAGESIZE, BLOCKSIZE};

pub struct FsPageManager {
    pages: File,
    count: Counter,
    last_page_last_block: BlockId,
    unpopulated_pages: Vec<u64>,   
}

impl FsPageManager {
    pub fn new(path: &Path) -> Self {
        FsPageManager {
            pages: OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(path)
                .unwrap(),
            count: Counter::new(),
            last_page_last_block: BlockId::last(),
            unpopulated_pages: Vec::new(),
        }
    }

    fn write_page(&mut self, page: Page, page_id: PageId) {
        let id = page_id.0;        
        let mut f = self.pages.try_clone().unwrap();
        f.seek(SeekFrom::Start(id * PAGESIZE as u64 * BLOCKSIZE as u64)).unwrap();
        f.write_all(page.as_slice()).unwrap();
        self.last_page_last_block = BlockId::last();
    }
}

impl PageStore for FsPageManager {

    fn store_full(&mut self, page: Page) -> PageId {
        let id = self.unpopulated_pages.pop().unwrap_or(self.count.retrieve_and_inc());        
        self.write_page(page, PageId(id));
        PageId(id)            
    } 

    fn store_unfull(&mut self, page: Page, block_id: BlockId) -> UnfullPage {
        let (page, page_id) = if self.last_page_last_block.0 + block_id.0 > PAGESIZE as u16 {
            //New Page
            self.last_page_last_block = BlockId(1);
            (Page::empty(), PageId(self.count.retrieve_and_inc()))
        } else {
            //Fits on same page
            let page_id = PageId(self.count.retrieve());
            (self.get_page(page_id), page_id) 
        };
        let first_block = self.last_page_last_block;
        //First byte of an unfull page acts as reference counter. 
        page[BlockId::first()].0[0] += 1;
        //Now copy the full blocks over to the unfull page
        for i in 0..block_id.0 {
            page[BlockId(first_block.0+i)] = page[BlockId(i)];
        }
        //Write the new page
        self.write_page(page, page_id);
        //And set the last_page_last_block
        self.last_page_last_block = BlockId(first_block.0 + block_id.0);
        UnfullPage(page_id, first_block, self.last_page_last_block)            
    }
    
    fn delete_page(&mut self, page_id: PageId) {
        let id = page_id.0;
        self.unpopulated_pages.push(id);
    }
    
    fn get_page(&self, page_id: PageId) -> Page {
        let mut f = self.pages.try_clone().unwrap();
        f.seek(SeekFrom::Start(page_id.0 * PAGESIZE as u64 * BLOCKSIZE as u64)).unwrap();
        Page::from_read(&mut f)
    }
}

#[cfg(test)]
mod tests {
    use test_utils::create_test_dir;

    use super::FsPageManager;
    use page_manager::{Page, PageStore, PageId, Block, BlockId, BLOCKSIZE};


    #[test]
    fn store_page() {
        let path = &create_test_dir("fs_page_manager/store_page");
        let mut pmgr = FsPageManager::new(&path.join("pages.bin"));
        assert_eq!(pmgr.store_full(Page::empty()), PageId(0));
    }



    #[test]
    fn delete_page() {
        let path = &create_test_dir("fs_page_manager/delete_page");
        let mut pmgr = FsPageManager::new(&path.join("pages.bin"));
        assert_eq!(pmgr.store_full(Page::empty()), PageId(0));
        assert_eq!(pmgr.store_full(Page::empty()), PageId(1));
        assert_eq!(pmgr.store_full(Page::empty()), PageId(2));
        assert_eq!(pmgr.store_full(Page::empty()), PageId(3));
        pmgr.delete_page(PageId(1));
        assert_eq!(pmgr.store_full(Page::empty()), PageId(1));
    }

    #[test]
    fn get_page() {
        let path = &create_test_dir("fs_page_manager/get_page");
        let mut pmgr = FsPageManager::new(&path.join("pages.bin"));
        assert_eq!(pmgr.store_full(Page::empty()), PageId(0));        
        assert_eq!(pmgr.get_page(PageId(0)), Page::empty());
        let mut p = Page::empty();
        p[BlockId::first()] = Block([1; BLOCKSIZE]);
        assert_eq!(pmgr.store_full(p), PageId(1));
        assert_eq!(pmgr.get_page(PageId(1)), p);
    }

    #[test]
    fn combined() {
        let path = &create_test_dir("fs_page_manager/combined");
        let mut pmgr = FsPageManager::new(&path.join("pages.bin"));
        let mut p = Page::empty();
        p[BlockId::first()] = Block([1; BLOCKSIZE]);
        p[BlockId(1)] = Block([2; BLOCKSIZE]);
        assert_eq!(pmgr.store_full(Page::empty()), PageId(0));
        assert_eq!(pmgr.store_full(p), PageId(1));
        assert_eq!(pmgr.store_full(p), PageId(2));
        p[BlockId(2)] = Block([3; BLOCKSIZE]);
        assert_eq!(pmgr.store_full(p), PageId(3));
        assert_eq!(pmgr.get_page(PageId(3)), p);
        assert!(pmgr.get_page(PageId(1)) != p);
        pmgr.delete_page(PageId(1));
        assert_eq!(pmgr.store_full(p), PageId(1));
        assert_eq!(pmgr.get_page(PageId(1)), p);
    }
}
