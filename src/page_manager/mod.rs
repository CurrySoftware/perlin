use std::sync::Arc;

pub use page_manager::page::{Page, PageId, PAGESIZE};
pub use page_manager::block::{Block, BlockId, BLOCKSIZE};

mod page;
mod block;
mod fs_page_manager;
mod ram_page_cache;

trait PageCache {
    fn get_page(&mut self, PageId) -> Arc<Page>;
    fn delete_page(&mut self, PageId);
}

trait PageStore {
    fn reserve_page(&mut self) -> PageId;
    fn store_reserved(&mut self, PageId, Page);
    fn get_page(&self, PageId) -> Page;
    fn delete_page(&mut self, PageId);
}

trait BlockManager {
    fn store_block(&mut self, Block) -> PageId;
    fn store_in_place(&mut self, PageId, BlockId, Block);
    fn flush_page(&mut self, PageId);
}



