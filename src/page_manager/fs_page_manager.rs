use std::path::Path;
use std::io::{Seek, SeekFrom, Write, Read};
use std::fs::{OpenOptions, File};

use utils::counter::Counter;
use page_manager::{UnfullPage, Page, PageId, BlockId, PageStore, PAGESIZE, BLOCKSIZE};

pub struct FsPageManager {
    pages: File,
    count: Counter,
    last_page_last_block: BlockId,
    unpopulated_pages: Vec<PageId>,
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
            last_page_last_block: BlockId(PAGESIZE as u16),
            unpopulated_pages: Vec::new(),
        }
    }

    fn write_page(&mut self, page: Page, page_id: PageId) {
        let id = page_id.0;
        let mut f = self.pages.try_clone().unwrap();
        f.seek(SeekFrom::Start(id * PAGESIZE as u64 * BLOCKSIZE as u64)).unwrap();
        f.write_all(page.as_slice()).unwrap();
        self.last_page_last_block = BlockId(PAGESIZE as u16);
    }
}

impl PageStore for FsPageManager {
    fn store_full(&mut self, page: Page) -> PageId {
        let id = self.unpopulated_pages.pop().unwrap_or(PageId(self.count.retrieve_and_inc()));
        self.write_page(page, id);
        id
    }

    fn store_unfull(&mut self, page: Page, block_id: BlockId) -> UnfullPage {
        let (mut container_page, page_id) = if self.last_page_last_block.0 + block_id.0 >
                                               PAGESIZE as u16 {
            // New Page
            self.last_page_last_block = BlockId(1);
            (Page::empty(), PageId(self.count.retrieve_and_inc()))
        } else {
            // Fits on same page
            let page_id = PageId(self.count.retrieve() - 1);
            (self.get_page(page_id), page_id)
        };
        let first_block = self.last_page_last_block;
        // First byte of an unfull page acts as reference counter.
        container_page[BlockId::first()].0[0] += 1;
        // Now copy the full blocks over to the unfull page
        for i in 0..block_id.0 {
            container_page[BlockId(first_block.0 + i)] = page[BlockId(i)];
        }
        // Write the new page
        self.write_page(container_page, page_id);
        // And set the last_page_last_block
        self.last_page_last_block = BlockId(first_block.0 + block_id.0);
        UnfullPage::new(page_id, first_block, self.last_page_last_block)
    }

    #[inline]
    fn delete_page(&mut self, page_id: PageId) {
        self.unpopulated_pages.push(page_id);
    }

    ///This method deletes an unfull page.
    ///Actually it just decreases the refcount of that page
    ///In case it becomes zero, the page is added to the unpopulated pages
    ///Otherwise the new refcount is written
    fn delete_unfull(&mut self, page_id: PageId) {
        let mut refcount: [u8; 1] = [0; 1];
        let mut f = self.pages.try_clone().unwrap();
        //Seek start of page
        f.seek(SeekFrom::Start(page_id.0 * PAGESIZE as u64 * BLOCKSIZE as u64)).unwrap();
        //Read one byte...
        f.read_exact(&mut refcount).unwrap();
        //Decrease it
        refcount[0] -= 1;
        //If its zero it means no more relevant data is on that page
        //Throw it into the unpopulated pages
        if refcount[0] == 0 {
            self.unpopulated_pages.push(page_id);
        } else {
            //Otherwise we have to write the refcount back to page... alas
            f.seek(SeekFrom::Start(page_id.0 * PAGESIZE as u64 * BLOCKSIZE as u64)).unwrap();
            f.write(&refcount).unwrap();
        }
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
    use page_manager::{UnfullPage, Page, PageStore, PageId, Block, BlockId, BLOCKSIZE, PAGESIZE};

    fn new_pmgr(name: &str) -> FsPageManager {
        let path = &create_test_dir(format!("fs_page_manager/{}", name).as_str());
        FsPageManager::new(&path.join("pages.bin"))
    }

    #[test]
    fn delete_unfull_basic() {
        let mut pmgr = new_pmgr("delete_unfull_basic");
        assert_eq!(pmgr.store_unfull(Page::empty(), BlockId(1)),
                   UnfullPage::new(PageId(0), BlockId(1), BlockId(2)));
        pmgr.delete_unfull(PageId(0));
        assert_eq!(pmgr.unpopulated_pages, vec![PageId(0)]);
        assert_eq!(pmgr.store_full(Page::empty()), PageId(0));
    }

    #[test]
    fn delete_multitenant_unfull() {
        let mut pmgr = new_pmgr("multitenant_unfull");
        let mut ref_p = Page::empty();
        for i in 0..PAGESIZE - 1 {
            ref_p[BlockId(i as u16 + 1u16)] = Block([(i % 255) as u8; BLOCKSIZE]);
            let mut p = Page::empty();
            p[BlockId::first()] = Block([(i % 255) as u8; BLOCKSIZE]);
            assert_eq!(pmgr.store_unfull(p, BlockId(1)),
                       UnfullPage::new(PageId(0), BlockId(i as u16 + 1), BlockId(i as u16 + 2)));
        }
        ref_p[BlockId::first()].0[0] = (PAGESIZE - 1) as u8;
        assert_eq!(pmgr.get_page(PageId(0)), ref_p);
        pmgr.delete_unfull(PageId(0));
        assert_eq!(pmgr.unpopulated_pages, vec![]);
        assert_eq!(pmgr.store_full(Page::empty()), PageId(1));
        for _ in 0..PAGESIZE -2 {
            assert_eq!(pmgr.unpopulated_pages, vec![]);
            pmgr.delete_unfull(PageId(0));
        }
        assert_eq!(pmgr.unpopulated_pages, vec![PageId(0)]);
        assert_eq!(pmgr.store_full(Page::empty()), PageId(0));
    }
    
    #[test]
    fn basic_unfull() {
        let mut pmgr = new_pmgr("basic_unfull");
        assert_eq!(pmgr.store_unfull(Page::empty(), BlockId(1)),
                   UnfullPage::new(PageId(0), BlockId(1), BlockId(2)));
        let mut p = Page::empty();
        p[BlockId::first()].0[0] = 1;
        assert_eq!(pmgr.get_page(PageId(0)), p);
    }

    #[test]
    fn filled_unfull() {
        let mut pmgr = new_pmgr("filled_unfull");
        let mut p = Page::empty();
        let mut ref_p = Page::empty();
        for i in 0..PAGESIZE - 1 {
            p[BlockId(i as u16)] = Block([(i % 255) as u8; BLOCKSIZE]);
            ref_p[BlockId(i as u16 + 1u16)] = Block([(i % 255) as u8; BLOCKSIZE]);
        }
        ref_p[BlockId::first()].0[0] = 1;
        assert_eq!(pmgr.store_unfull(p, BlockId::last()),
                   UnfullPage::new(PageId(0), BlockId(1), BlockId(PAGESIZE as u16)));
        assert_eq!(pmgr.get_page(PageId(0)), ref_p);
    }

    #[test]
    fn multitenant_unfull() {
        let mut pmgr = new_pmgr("multitenant_unfull");
        let mut ref_p = Page::empty();
        for i in 0..PAGESIZE - 1 {
            ref_p[BlockId(i as u16 + 1u16)] = Block([(i % 255) as u8; BLOCKSIZE]);
            let mut p = Page::empty();
            p[BlockId::first()] = Block([(i % 255) as u8; BLOCKSIZE]);
            assert_eq!(pmgr.store_unfull(p, BlockId(1)),
                       UnfullPage::new(PageId(0), BlockId(i as u16 + 1), BlockId(i as u16 + 2)));
        }
        ref_p[BlockId::first()].0[0] = (PAGESIZE - 1) as u8;
        assert_eq!(pmgr.get_page(PageId(0)), ref_p);
    }

    #[test]
    fn overflowing_unfull() {
        let mut pmgr = new_pmgr("overflowing_unfull");
        let mut ref_p = Page::empty();
        for i in 0..PAGESIZE - 1 {
            ref_p[BlockId(i as u16 + 1u16)] = Block([(i % 255) as u8; BLOCKSIZE]);
            let mut p = Page::empty();
            p[BlockId::first()] = Block([(i % 255) as u8; BLOCKSIZE]);
            assert_eq!(pmgr.store_unfull(p, BlockId(1)),
                       UnfullPage::new(PageId(0), BlockId(i as u16 + 1), BlockId(i as u16 + 2)));
        }
        for i in 0..PAGESIZE - 1 {
            let mut p = Page::empty();
            p[BlockId::first()] = Block([(i % 255) as u8; BLOCKSIZE]);
            assert_eq!(pmgr.store_unfull(p, BlockId(1)),
                       UnfullPage::new(PageId(1), BlockId(i as u16 + 1), BlockId(i as u16 + 2)));
        }
        ref_p[BlockId::first()].0[0] = (PAGESIZE - 1) as u8;
        assert_eq!(pmgr.get_page(PageId(0)), ref_p);
        assert_eq!(pmgr.get_page(PageId(1)), ref_p);
    }

    #[test]
    fn unfull_after_full() {
        let mut pmgr = new_pmgr("unfull_after_full");
        let mut p = Page::empty();
        p[BlockId::first()] = Block([1; BLOCKSIZE]);
        assert_eq!(pmgr.store_full(p), PageId(0));
        assert_eq!(pmgr.get_page(PageId(0)), p);
        assert_eq!(pmgr.store_unfull(Page::empty(), BlockId(1)),
                   UnfullPage::new(PageId(1), BlockId(1), BlockId(2)));
        let mut unf_p = Page::empty();
        unf_p[BlockId::first()].0[0] = 1;
        assert_eq!(pmgr.get_page(PageId(1)), unf_p);
        assert_eq!(pmgr.store_full(p), PageId(2));
        assert_eq!(pmgr.get_page(PageId(2)), p);
        assert_eq!(pmgr.store_unfull(Page::empty(), BlockId(1)),
                   UnfullPage::new(PageId(3), BlockId(1), BlockId(2)));
        assert_eq!(pmgr.get_page(PageId(1)), unf_p);
    }


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
