use std::path::Path;
use std::io::{Seek, SeekFrom, Write};
use std::fs::{OpenOptions, File};

use page_manager::{Page, PageId, PageStore, PAGESIZE, BLOCKSIZE};

struct FsPageManager {
    pages: File,
    count: u64,
    unpopulated_pages: Vec<u64>,
}

impl FsPageManager {
    fn new(path: &Path) -> Self {
        FsPageManager {
            pages: OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(path)
                .unwrap(),
            count: 0,
            unpopulated_pages: Vec::new(),
        }
    }

    #[cfg(test)]
    fn store_page(&mut self, page: Page) -> PageId {
        let id = self.reserve_page();
        self.store_reserved(id, page);
        id
    }
}

impl PageStore for FsPageManager {

    fn store_reserved(&mut self, page_id: PageId, page: Page) {
        let id = page_id.0;
        let mut f = self.pages.try_clone().unwrap();
        f.seek(SeekFrom::Start(id * PAGESIZE as u64 * BLOCKSIZE as u64)).unwrap();
        f.write_all(page.as_slice()).unwrap();
    } 
    
    fn reserve_page(&mut self) -> PageId {
        if !self.unpopulated_pages.is_empty() {
            let id = self.unpopulated_pages.swap_remove(0);
            PageId(id)
        } else {
            let id = PageId(self.count);
            self.count += 1;
            id
        }
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
        assert_eq!(pmgr.store_page(Page::empty()), PageId(0));
    }



    #[test]
    fn delete_page() {
        let path = &create_test_dir("fs_page_manager/delete_page");
        let mut pmgr = FsPageManager::new(&path.join("pages.bin"));
        assert_eq!(pmgr.store_page(Page::empty()), PageId(0));
        assert_eq!(pmgr.store_page(Page::empty()), PageId(1));
        assert_eq!(pmgr.store_page(Page::empty()), PageId(2));
        assert_eq!(pmgr.store_page(Page::empty()), PageId(3));
        pmgr.delete_page(PageId(1));
        assert_eq!(pmgr.store_page(Page::empty()), PageId(1));
    }

    #[test]
    fn get_page() {
        let path = &create_test_dir("fs_page_manager/get_page");
        let mut pmgr = FsPageManager::new(&path.join("pages.bin"));
        assert_eq!(pmgr.store_page(Page::empty()), PageId(0));        
        assert_eq!(pmgr.get_page(PageId(0)), Page::empty());
        let mut p = Page::empty();
        p[BlockId::first()] = Block([1; BLOCKSIZE]);
        assert_eq!(pmgr.store_page(p), PageId(1));
        assert_eq!(pmgr.get_page(PageId(1)), p);
    }

    #[test]
    fn combined() {
        let path = &create_test_dir("fs_page_manager/combined");
        let mut pmgr = FsPageManager::new(&path.join("pages.bin"));
        let mut p = Page::empty();
        p[BlockId::first()] = Block([1; BLOCKSIZE]);
        p[BlockId(1)] = Block([2; BLOCKSIZE]);
        assert_eq!(pmgr.store_page(Page::empty()), PageId(0));
        assert_eq!(pmgr.store_page(p), PageId(1));
        assert_eq!(pmgr.store_page(p), PageId(2));
        p[BlockId(2)] = Block([3; BLOCKSIZE]);
        assert_eq!(pmgr.store_page(p), PageId(3));
        assert_eq!(pmgr.get_page(PageId(3)), p);
        assert!(pmgr.get_page(PageId(1)) != p);
        pmgr.delete_page(PageId(1));
        assert_eq!(pmgr.store_page(p), PageId(1));
        assert_eq!(pmgr.get_page(PageId(1)), p);
    }
}
