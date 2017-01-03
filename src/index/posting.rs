use compressor::Compressor;
use page_manager::BlockIter;
use utils::ring_buffer::BiasedRingBuffer;
use utils::Baseable;
use index::listing::UsedCompressor;


#[derive(Debug, PartialEq, Copy, Clone)]
pub struct Posting(pub DocId);
#[derive(Debug, PartialEq, Copy, Clone)]
pub struct DocId(pub u64);

impl DocId {
    #[inline]
    pub fn none() -> DocId {
        DocId(u64::max_value())
    }
}

impl<'a> Baseable<&'a DocId> for DocId {
    #[inline]
    fn base_on(&mut self, other: &Self) {
        self.0 -= other.0
    }
}

impl Posting {
    #[inline]
    pub fn none() -> Posting {
        Posting(DocId::none())
    }
}

impl Default for Posting {
    fn default() -> Self {
        Posting(DocId(0))
    }
}

impl<'a> Baseable<&'a Posting> for Posting {
    #[inline]
    fn base_on(&mut self, other: &Self) {
        self.0.base_on(&other.0);
    }
}


pub struct PostingIterator<'a> {
    blocks: BlockIter<'a>,
    bias_list: Vec<Posting>,
    bias_list_ptr: usize,
    posting_buffer: BiasedRingBuffer<Posting>,
}

impl<'a> PostingIterator<'a> {
    pub fn new(blocks: BlockIter<'a>, bias_list: Vec<Posting>) -> Self {
        PostingIterator {
            blocks: blocks,
            bias_list: bias_list,
            bias_list_ptr: 0,
            posting_buffer: BiasedRingBuffer::new(),
        }
    }
}

impl<'a> Iterator for PostingIterator<'a> {
    type Item = Posting;

    fn next(&mut self) -> Option<Posting> {
        if self.posting_buffer.is_empty() {
            if let Some(block) = self.blocks.next() {
                let bias = self.bias_list[self.bias_list_ptr];
                self.bias_list_ptr += 1;
                self.posting_buffer.base_on(bias);
                UsedCompressor::decompress(block, &mut self.posting_buffer);
            }
        }
        self.posting_buffer.pop_front()
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    use index::listing::Listing;
    use page_manager::{FsPageManager, RamPageCache};


    use test_utils::create_test_dir;

    fn new_cache(name: &str) -> RamPageCache {
        let path = &create_test_dir(format!("posting/{}", name).as_str());
        let pmgr = FsPageManager::new(&path.join("pages.bin"));
        RamPageCache::new(pmgr)
    }

    #[test]
    fn single() {
        let mut cache = new_cache("single");
        let mut listing = Listing::new();
        listing.add(&[Posting(DocId(0))], &mut cache);
        listing.commit(&mut cache);
        assert_eq!(listing.posting_iter(&cache).collect::<Vec<_>>(),
                   vec![Posting(DocId(0))]);
    }

    #[test]
    fn many() {
        let mut cache = new_cache("many");
        let mut listing = Listing::new();
        for i in 0..2048 {
            listing.add(&[Posting(DocId(i))], &mut cache);
        }
        listing.commit(&mut cache);
        let res = (0..2048).map(|i| Posting(DocId(i))).collect::<Vec<_>>();
        assert_eq!(listing.posting_iter(&cache).collect::<Vec<_>>(), res);
    }

    #[test]
    fn multiple_listings() {
        let mut cache = new_cache("multiple_listings");
        let mut listing1 = Listing::new();
        let mut listing2 = Listing::new();
        let mut listing3 = Listing::new();
        for i in 0..2049 {
            listing1.add(&[Posting(DocId(i))], &mut cache);
            listing2.add(&[Posting(DocId(i*2))], &mut cache);
            listing3.add(&[Posting(DocId(i*3))], &mut cache);
        }
        listing1.commit(&mut cache);
        listing2.commit(&mut cache);
        listing3.commit(&mut cache);
        let res1 = (0..2049).map(|i| Posting(DocId(i))).collect::<Vec<_>>();
        let res2 = (0..2049).map(|i| Posting(DocId(i*2))).collect::<Vec<_>>();
        let res3 = (0..2049).map(|i| Posting(DocId(i*3))).collect::<Vec<_>>();
        assert_eq!(listing1.posting_iter(&cache).collect::<Vec<_>>(), res1);
        assert_eq!(listing2.posting_iter(&cache).collect::<Vec<_>>(), res2);
        assert_eq!(listing3.posting_iter(&cache).collect::<Vec<_>>(), res3);
    }

    #[test]
    fn different_listings() {
        let mut cache = new_cache("different_listings");
        let mut listing1 = Listing::new();
        let mut listing2 = Listing::new();
        let mut listing3 = Listing::new();
        for i in 0..4596 {            
            listing1.add(&[Posting(DocId(i))], &mut cache);
            if i % 2 == 0 {
                listing2.add(&[Posting(DocId(i*2))], &mut cache);
            }
            if i % 3 == 0 {
                listing3.add(&[Posting(DocId(i*3))], &mut cache);
            }
        }
        listing1.commit(&mut cache);
        listing2.commit(&mut cache);
        listing3.commit(&mut cache);
        let res1 = (0..4596).map(|i| Posting(DocId(i))).collect::<Vec<_>>();
        let res2 = (0..4596).filter(|i| i % 2 == 0).map(|i| Posting(DocId(i*2))).collect::<Vec<_>>();
        let res3 = (0..4596).filter(|i| i % 3 == 0).map(|i| Posting(DocId(i*3))).collect::<Vec<_>>();
        assert_eq!(listing1.posting_iter(&cache).collect::<Vec<_>>(), res1);
        // assert_eq!(listing2.posting_iter(&cache).collect::<Vec<_>>(), res2);
        // assert_eq!(listing3.posting_iter(&cache).collect::<Vec<_>>(), res3);
    }


  
}
