use std::sync::Arc;

pub use page_manager::page::{Pages, UnfullPage, Page, PageId, PAGESIZE};
pub use page_manager::block::{Block, BlockId, BLOCKSIZE};
pub use page_manager::fs_page_manager::FsPageManager;
pub use page_manager::ram_page_cache::RamPageCache;
pub use page_manager::block_iter::BlockIter;

mod page;
mod block;
mod fs_page_manager;
mod ram_page_cache;
mod block_iter;

pub trait PageCache {
    fn get_page(&self, PageId) -> Arc<Page>;
    fn delete_page(&mut self, PageId);
    fn delete_unfull(&mut self, PageId);
}

trait PageStore {
    fn store_unfull(&mut self, Page, BlockId) -> UnfullPage;
    fn store_full(&mut self, Page) -> PageId;
    fn get_page(&self, PageId) -> Page;
    fn delete_page(&mut self, PageId);
    fn delete_unfull(&mut self, PageId);
}

pub trait BlockManager {
    fn store_block(&mut self, Block) -> PageId;
    fn store_in_place(&mut self, PageId, BlockId, Block);
    fn flush_page(&mut self, PageId) -> PageId;
    fn flush_unfull(&mut self, PageId, BlockId) -> UnfullPage;    
}



