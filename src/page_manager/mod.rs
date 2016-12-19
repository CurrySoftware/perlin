use std::sync::Arc;

pub use page_manager::page::{Page, PageId, PAGESIZE};
pub use page_manager::block::{Block, BlockId, BLOCKSIZE};

mod page;
mod block;
mod ram_page_manager;
mod fs_page_manager;


trait PageManager {
    fn store_page(&mut self, Page) -> PageId;
    fn delete_page(&mut self, PageId);
}

trait PageCache {
    fn get_page(&self, PageId) -> Arc<Page>;
}

trait PageStore {
    fn get_page(&self, PageId) -> Page;
}

trait BlockManager {
    fn store_block(&mut self, Block) -> PageId;
    fn store_in_page(&mut self, PageId, Block) -> Result<BlockId, PageId>;
}



