use index::boolean_index::posting::Listing;

pub struct IndexingChunk{
    last_doc_id: u64,
    len: u16,
    capa: u16,
    data: [u8; 4092]
}


impl IndexingChunk {
    /// Adds listing to IndexingChunk. Returns Ok if listing fits into chunk
    /// Otherwise returns the encoded and compressed data to be used in the next chunk
    fn add_listing(&mut self, listing: Listing) -> Result<(), Vec<u8>> {
        // for posting in listing {
            
        // }
        // let bytes = self.encode_listing(listing);
        // if bytes.len() > self.capa {
        //     Err(bytes)
        // } else {
            
        // }
        Ok(())
    }

    fn encode_listing(&self, listing: Listing) -> Vec<u8> {
        vec![]
    }

}
