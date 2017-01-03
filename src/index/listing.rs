use utils::ring_buffer::BiasedRingBuffer;
use utils::Baseable;

use compressor::{Compressor, NaiveCompressor};

use page_manager::{Pages, PageId, Block, BlockIter, BlockId, RamPageCache, BlockManager};

use index::posting::{Posting, DocId, PostingIterator};

pub type UsedCompressor = NaiveCompressor;

pub struct Listing {
    pages: Pages,
    current_page: Option<PageId>,
    block_biases: Vec<Posting>,
    block_counter: BlockId,
    block_start: Posting,
    block_end: Posting,
    posting_buffer: BiasedRingBuffer<Posting>,
}

impl Listing {
    pub fn new() -> Self {
        Listing {
            pages: Pages::new(),
            current_page: None,
            block_biases: Vec::new(),
            block_counter: BlockId::first(),
            posting_buffer: BiasedRingBuffer::new(),
            block_start: Posting(DocId(0)),
            block_end: Posting(DocId(0)),
        }
    }

    pub fn add(&mut self, postings: &[Posting], page_cache: &mut RamPageCache) {
        for (i, posting) in postings.iter().enumerate() {
            self.block_end = *posting;
            self.posting_buffer.push_back(*posting);
            if i % 8 == 0 {
                self.compress_and_ship(page_cache, false);
            }
        }
        self.compress_and_ship(page_cache, false);
    }

    pub fn commit(&mut self, page_cache: &mut RamPageCache) {
        self.compress_and_ship(page_cache, true);
        // Currentpage has to be unfull. Or None
        if let Some(unfull_page) = self.current_page.take() {
            self.pages.add_unfull(page_cache.flush_unfull(unfull_page, self.block_counter));
            self.block_counter = BlockId::first();
        }
    }


    pub fn posting_iter<'a>(&self, cache: &'a RamPageCache) -> PostingIterator<'a> {
        let block_iter = BlockIter::new(cache, self.pages.clone());
        PostingIterator::new(block_iter, self.block_biases.clone())
    }

    fn last_block_id(&self) -> BlockId {
        let mut r = self.block_counter;
        r.dec();
        r
    }

    fn compress_and_ship(&mut self, page_cache: &mut RamPageCache, force: bool) {
        while let Some(block) = UsedCompressor::compress(&mut self.posting_buffer) {
            self.ship(page_cache, block);
        }
        if force && self.posting_buffer.count() > 0 {
            let block = UsedCompressor::force_compress(&mut self.posting_buffer);
            self.ship(page_cache, block);
        }
    }

    /// This method does three things:
    /// 1. It pushes a block to an existing page in ram or demands for one to
    /// be created
    /// 2. It checks if that page is full and then tells the ramcache to flush
    /// it
    /// 3. It defines the bounds of the block (e.g. With what docid does a
    /// block start) and stores these
    fn ship(&mut self, page_cache: &mut RamPageCache, block: Block) {
        // If the block is on a new page
        if self.block_counter == BlockId::first() {
            // Push it on a new page and store the page
            self.current_page = Some(page_cache.store_block(block));
        } else {
            // Otherwise store it on an existing page
            page_cache.store_in_place(self.current_page.unwrap(), self.block_counter, block)
        }
        // Save with what doc_id the block just stored block starts
        self.block_biases.push(self.block_start);
        // We just wrote the last block of a page. Flush it!
        if self.block_counter == BlockId::last() {
            // Store page, turn current_page to none
            self.pages.push(page_cache.flush_page(self.current_page.take().unwrap()));
        }
        // Count up the block
        self.block_counter.inc();

        // If we know the next element take that as bias. Otherwise take the last of
        // this block
        if let Some(posting) = self.posting_buffer.peek_front() {
            self.block_start = *posting;
        } else {
            self.block_start = self.block_end;
        }

        self.posting_buffer.base_on(self.block_start);
    }
}


#[cfg(test)]
mod tests {

    use super::Listing;

    use test_utils::create_test_dir;

    use index::posting::{Posting, DocId};
    use page_manager::{FsPageManager, RamPageCache, BlockId};


    fn new_cache(name: &str) -> RamPageCache {
        let path = &create_test_dir(format!("listing/{}", name).as_str());
        let pmgr = FsPageManager::new(&path.join("pages.bin"));
        RamPageCache::new(pmgr)
    }

    #[test]
    fn basic_add() {
        let mut cache = new_cache("basic_add");
        let mut listing = Listing::new();
        listing.add(&[Posting(DocId(0))], &mut cache);
        assert_eq!(listing.pages.len(), 0);
        assert_eq!(listing.posting_buffer.count(), 1);
    }

    #[test]
    fn commit() {
        let mut cache = new_cache("commit");
        let mut listing = Listing::new();
        listing.add(&[Posting(DocId(0))], &mut cache);
        assert_eq!(listing.pages.len(), 0);
        assert_eq!(listing.posting_buffer.count(), 1);
        listing.commit(&mut cache);
        assert_eq!(listing.pages.len(), 1);
        assert_eq!(listing.posting_buffer.count(), 0);
        assert_eq!(listing.last_block_id(), BlockId::first());
    }


    #[test]
    fn add() {
        let mut cache = new_cache("add");
        let mut listing = Listing::new();
        listing.add(&[Posting(DocId(0))], &mut cache);
        assert_eq!(listing.pages.len(), 0);
        assert_eq!(listing.posting_buffer.count(), 1);
        for i in 0..100 {
            listing.add(&[Posting(DocId(i))], &mut cache);
        }
        assert_eq!(listing.pages.len(), 0);
        assert!(listing.posting_buffer.count() > 0);
        listing.commit(&mut cache);
        assert_eq!(listing.posting_buffer.count(), 0);
    }

    #[test]
    fn add_much() {
        let mut cache = new_cache("add_much");
        let mut listing = Listing::new();
        listing.add(&[Posting(DocId(0))], &mut cache);
        assert_eq!(listing.pages.len(), 0);
        assert_eq!(listing.posting_buffer.count(), 1);
        for i in 0..10000 {
            listing.add(&[Posting(DocId(i))], &mut cache);
        }
        assert!(listing.pages.len() > 0);
        assert!(listing.posting_buffer.count() > 0);
        listing.commit(&mut cache);
        assert_eq!(listing.posting_buffer.count(), 0);
    }

    #[test]
    fn multiple_listings() {
        let mut cache = new_cache("multiple_listings");
        let mut listings = (0..100).map(|_| Listing::new()).collect::<Vec<_>>();
        for i in 0..50000 {
            listings[i % 100].add(&[Posting(DocId(i as u64))], &mut cache);
        }
        for listing in listings.iter_mut() {
            assert!(listing.posting_buffer.count() > 0);
            listing.commit(&mut cache);
        }
        for listing in listings {
            assert_eq!(listing.posting_buffer.count(), 0);
        }
    }

    #[test]
    fn biases() {
        let mut cache = new_cache("biases");
        let mut listing = Listing::new();

        listing.add(&[Posting(DocId(1))], &mut cache);
        assert_eq!(listing.block_start, Posting(DocId(0)));
        assert_eq!(listing.block_end, Posting(DocId(1)));
        listing.commit(&mut cache);
        assert_eq!(listing.block_start, Posting(DocId(1)));
        assert_eq!(listing.block_end, Posting(DocId(1)));
        listing.add(&[Posting(DocId(10))], &mut cache);
        assert_eq!(listing.block_end, Posting(DocId(10)));
        listing.commit(&mut cache);
        assert_eq!(listing.block_start, Posting(DocId(10)));
        assert_eq!(listing.block_biases,
                   vec![Posting(DocId(0)), Posting(DocId(1))]);
    }
}
