use std::thread;
use std::sync::mpsc;
use std::sync::Arc;
use std::sync::atomic::{Ordering, AtomicUsize};
use std::hash::Hash;
use std::collections::HashMap;


use storage::compression::VByteEncoded;

use index::boolean_index::{Result, Error, IndexingError, DocumentTerms};
use index::boolean_index::posting::{Posting, Listing};
use chunked_storage::{ChunkedStorage, IndexingChunk};
use chunked_storage::chunk_ref::MutChunkRef;
use storage::Storage;

const SORT_THREADS: usize = 4;

/// Indexes a document collection for later retrieval
/// Returns the number of documents indexed
pub fn index_documents<TDocsIterator, TDocIterator, TStorage, TDocStorage, TTerm>
    (documents: TDocsIterator,
     storage: TStorage,
     doc_storage: TDocStorage)
     -> Result<(usize, ChunkedStorage, TDocStorage, HashMap<TTerm, u64>)>
    where TDocsIterator: Iterator<Item = TDocIterator>,
          TDocIterator: Iterator<Item = TTerm>,
          TStorage: Storage<IndexingChunk> + 'static,
          TDocStorage: Storage<DocumentTerms> + 'static,
          TTerm: Ord + Hash
{
    // Channel for sorting-thread <-> inverting-thread communication
    let (merged_tx, merged_rx) = mpsc::sync_channel(64);
    // Channel for index-thread <-> document-storage-thread
    let (doc_store_tx, doc_store_rx) = mpsc::channel();
    let mut document_count = 0;
    let thread_sync = Arc::new(AtomicUsize::new(0));
    // Initialize and start sorting threads
    let mut chunk_tx = Vec::with_capacity(SORT_THREADS);
    let mut sort_threads = Vec::with_capacity(SORT_THREADS);
    for _ in 0..SORT_THREADS {
        let (tx, rx) = mpsc::sync_channel(4);
        chunk_tx.push(tx);
        let m_tx = merged_tx.clone();
        let loc_sync = thread_sync.clone();
        sort_threads.push(thread::spawn(|| sort_and_group_chunk(loc_sync, rx, m_tx)));
    }
    drop(merged_tx);
    let inv_index = thread::spawn(|| invert_index(merged_rx, storage));
    let doc_store = thread::spawn(|| store_documents(doc_storage, doc_store_rx));
    let mut term_ids: HashMap<TTerm, u64> = HashMap::new();
    let mut buffer = Vec::with_capacity(213400);
    let mut term_count = 0;
    
    //Start Work: For every document in the collection
    let mut chunk_count = 0;
    for (doc_id, document) in documents.enumerate() {
        let mut doc_terms = Vec::with_capacity(1000);
        // Enumerate over its terms        
        for (_, term) in document.into_iter().enumerate() {
            // Has term already been seen? Is it already in the vocabulary?
            if let Some(term_id) = term_ids.get(&term) {
                buffer.push((*term_id, doc_id as u64));
                doc_terms.push(*term_id);
                continue;
            }
            term_ids.insert(term, term_count as u64);
            buffer.push((term_count as u64, doc_id as u64));
            doc_terms.push(term_count as u64);
            term_count += 1;
        }
        // Term was not yet indexed. Add it
        document_count += 1;
        if document_count % 256 == 0 {
            let index = chunk_count % SORT_THREADS;
            let old_len = buffer.len();
            try!(chunk_tx[index].send((chunk_count, buffer)));
            buffer = Vec::with_capacity(old_len + old_len / 10);
            chunk_count += 1;
        }
        doc_store_tx.send((doc_id as u64, doc_terms))?;
    }
    try!(chunk_tx[chunk_count % SORT_THREADS].send((chunk_count, buffer)));
    drop(chunk_tx);
    drop(doc_store_tx);
    // Join sort threads
    if sort_threads.into_iter().any(|thread| thread.join().is_err()) {
        return Err(Error::Indexing(IndexingError::ThreadPanic));
    }
    // Join invert index thread and save result
    let chunked_postings = match inv_index.join() {
        Ok(res) => try!(res),
        Err(_) => return Err(Error::Indexing(IndexingError::ThreadPanic)),
    };
    
    let doc_storage = match doc_store.join() {
        Ok(res) => res?,
        Err(_) => return Err(Error::Indexing(IndexingError::ThreadPanic))
    };

    Ok((document_count, chunked_postings, doc_storage, term_ids))
}

fn store_documents<TDocStorage>(mut doc_storage: TDocStorage, documents: mpsc::Receiver<(u64, Vec<u64>)>) -> Result<TDocStorage>
    where TDocStorage: Storage<DocumentTerms> {
    while let Ok((doc_id, terms)) = documents.recv() {
        let mut bytes = Vec::with_capacity(terms.len()*4);
        for term in terms {
            VByteEncoded::new(term as usize).write_to(&mut bytes)?;
        }
        doc_storage.store(doc_id, bytes)?;
    }
    Ok(doc_storage)
}

/// Receives chunks of (`term_id`, `doc_id`) tripels
/// Sorts and groups them by `term_id` then sends them
fn sort_and_group_chunk(sync: Arc<AtomicUsize>,
                        ids: mpsc::Receiver<(usize, Vec<(u64, u64)>)>,
                        grouped_chunks: mpsc::SyncSender<Vec<(u64, Listing)>>) {

    while let Ok((id, mut chunk)) = ids.recv() {
        // Sort triples by term_id
        chunk.sort_by_key(|&(a, _)| a);
        chunk.dedup();
        let mut grouped_chunk = Vec::with_capacity(chunk.len());
        let mut last_tid = 0;
        let mut term_counter = 0;
        // Group by term_id and doc_id
        for (i, &(term_id, doc_id)) in chunk.iter().enumerate() {
            // if term is the first term or different to the last term (new group)
            if last_tid < term_id || i == 0 {
                term_counter += 1;
                // Term_id has to be added
                grouped_chunk.push((term_id, vec![Posting::new(doc_id)]));
                last_tid = term_id;
                continue;
            }          
            // Otherwise add a whole new posting
            grouped_chunk[term_counter - 1].1.push(Posting::new(doc_id));
        }
        // Send grouped chunk to merger thread. Make sure to send chunks in right order
        // (yes, this is a verb: https://en.wiktionary.org/wiki/grouped#English)
        loop {
            let atm = sync.load(Ordering::SeqCst);
            if atm == id {
                grouped_chunks.send(grouped_chunk).unwrap();
                sync.fetch_add(1, Ordering::SeqCst);
                break;
            }
        }
    }
}

fn invert_index<TStorage>(grouped_chunks: mpsc::Receiver<Vec<(u64, Listing)>>,
                          storage: TStorage)
                          -> Result<ChunkedStorage>
    where TStorage: Storage<IndexingChunk> + 'static
{
    let mut storage = ChunkedStorage::new(10000, Box::new(storage));
    while let Ok(chunk) = grouped_chunks.recv() {
        let threshold = storage.len();
        for (term_id, listing) in chunk {
            let uterm_id = term_id as usize;
            // Get chunk to write to or create if unknown
            let mut stor_chunk = if uterm_id < threshold {
                storage.get_mut(term_id)
            } else {
                storage.new_chunk(term_id)
            };
            let base_doc_id = stor_chunk.get_last_doc_id();
            try!(write_listing(listing, base_doc_id, &mut stor_chunk));
        }
    }
    Ok(storage)
}

fn write_listing(listing: Listing, mut base_doc_id: u64, target: &mut MutChunkRef) -> Result<u64> {
    for Posting(doc_id) in listing {
        // Delta encode
        let delta_doc_id = doc_id - base_doc_id;
        // Update base id
        base_doc_id = doc_id;
        let data = VByteEncoded::new(delta_doc_id as usize);

        target.write_posting(base_doc_id, data.data_buf())?;
    }
    Ok(base_doc_id)
}


#[cfg(test)]
mod tests {

    use std::thread;
    use std::sync::mpsc;
    use std::sync::Arc;
    use std::sync::atomic::AtomicUsize;

    use storage::persistence::Volatile;
    use storage::compression::VByteDecoder;
    use chunked_storage::ChunkedStorage;
    use index::boolean_index::posting::{Posting, PostingDecoder};
    use storage::RamStorage;

    #[test]
    fn basic_sorting() {
        let (trp_tx, trp_rx) = mpsc::channel();
        let (sorted_tx, sorted_rx) = mpsc::sync_channel(64);
        let sync = Arc::new(AtomicUsize::new(0));
        thread::spawn(|| super::sort_and_group_chunk(sync, trp_rx, sorted_tx));

        // (term_id, doc_id, position)
        // Document 0: "0, 0, 1"
        // Document 1: "0"
        trp_tx.send((0, vec![(0, 0), (0, 0), (1, 0), (0, 1)])).unwrap();
        assert_eq!(sorted_rx.recv().unwrap(),
                   vec![(0, vec![Posting::new(0), Posting::new(1)]),
                        (1, vec![Posting::new(0)])]);
    }

    #[test]
    fn extended_sorting() {
        let (trp_tx, trp_rx) = mpsc::channel();
        let (sorted_tx, sorted_rx) = mpsc::sync_channel(64);
        let sync = Arc::new(AtomicUsize::new(0));
        thread::spawn(|| super::sort_and_group_chunk(sync, trp_rx, sorted_tx));

        trp_tx.send((0, (0..100).map(|i| (i, i)).collect::<Vec<_>>())).unwrap();
        let sorted = sorted_rx.recv().unwrap();
        assert_eq!(sorted,
                   (0..100).map(|i| (i, vec![Posting::new(i)])).collect::<Vec<_>>());

        trp_tx.send((1, (0..100).map(|i| (i, i)).collect::<Vec<_>>())).unwrap();
        let sorted = sorted_rx.recv().unwrap();
        assert_eq!(sorted,
                   (0..100).map(|i| (i, vec![Posting::new(i)])).collect::<Vec<_>>());

        trp_tx.send((2, (200..300).map(|i| (i, i)).collect::<Vec<_>>())).unwrap();
        let sorted = sorted_rx.recv().unwrap();
        assert_eq!(sorted,
                   (200..300).map(|i| (i, vec![Posting::new(i)])).collect::<Vec<_>>());
    }

    #[test]
    fn multi_sorting() {
        let (sorted_tx, sorted_rx) = mpsc::sync_channel(64);
        let sync = Arc::new(AtomicUsize::new(0));
        for i in 0..2 {
            let (trp_tx, trp_rx) = mpsc::channel();
            let local_sync = sync.clone();
            let loc_tx = sorted_tx.clone();
            thread::spawn(|| super::sort_and_group_chunk(local_sync, trp_rx, loc_tx));
            trp_tx.send((i, (i as u64..100).map(|k| (k, k)).collect::<Vec<_>>())).unwrap();
        }

        let sorted = sorted_rx.recv().unwrap();
        assert_eq!(sorted,
                   (0..100).map(|i| (i, vec![Posting::new(i)])).collect::<Vec<_>>());
        let sorted = sorted_rx.recv().unwrap();
        assert_eq!(sorted,
                   (1..100).map(|i| (i, vec![Posting::new(i)])).collect::<Vec<_>>());
    }

    #[test]
    fn multi_sorting_asymetric() {
        let (sorted_tx, sorted_rx) = mpsc::sync_channel(64);
        let sync = Arc::new(AtomicUsize::new(0));
        for i in 0..2 {
            let (trp_tx, trp_rx) = mpsc::channel();
            let local_sync = sync.clone();
            let loc_tx = sorted_tx.clone();
            thread::spawn(|| super::sort_and_group_chunk(local_sync, trp_rx, loc_tx));
            if i == 0 {
                trp_tx.send((i, (i as u64..10000).map(|k| (k, k)).collect::<Vec<_>>())).unwrap();
            } else {
                trp_tx.send((i, (i as u64..10).map(|k| (k, k)).collect::<Vec<_>>())).unwrap();
            }
        }

        let sorted = sorted_rx.recv().unwrap();
        assert_eq!(sorted,
                   (0..10000).map(|i| (i, vec![Posting::new(i)])).collect::<Vec<_>>());
        let sorted = sorted_rx.recv().unwrap();
        assert_eq!(sorted,
                   (1..10).map(|i| (i, vec![Posting::new(i)])).collect::<Vec<_>>());
    }

    #[test]
    fn multi_sorting_messedup() {
        let (sorted_tx, sorted_rx) = mpsc::sync_channel(64);
        let sync = Arc::new(AtomicUsize::new(0));
        for i in 0..2 {
            let (trp_tx, trp_rx) = mpsc::channel();
            let local_sync = sync.clone();
            let loc_tx = sorted_tx.clone();
            thread::spawn(|| super::sort_and_group_chunk(local_sync, trp_rx, loc_tx));
            trp_tx.send((1 - i, (i as u64..100).map(|k| (k, k)).collect::<Vec<_>>())).unwrap();
        }

        let sorted = sorted_rx.recv().unwrap();
        assert_eq!(sorted,
                   (1..100).map(|i| (i, vec![Posting::new(i)])).collect::<Vec<_>>());
        let sorted = sorted_rx.recv().unwrap();
        assert_eq!(sorted,
                   (0..100).map(|i| (i, vec![Posting::new(i)])).collect::<Vec<_>>());
    }


    #[test]
    fn basic_inverting() {
        let (sorted_tx, sorted_rx) = mpsc::sync_channel(64);
        let result = thread::spawn(|| super::invert_index(sorted_rx, RamStorage::new()));

        sorted_tx.send((0..100).map(|i| (i, vec![Posting::new(i)])).collect::<Vec<_>>()).unwrap();
        drop(sorted_tx);

        let chunked_storage = result.join().unwrap().unwrap();
        assert_eq!(chunked_storage.len(), 100);
        assert_eq!(PostingDecoder::new(chunked_storage.get(0)).collect::<Vec<_>>(),
                   vec![Posting::new(0)]);
        assert_eq!(PostingDecoder::new(chunked_storage.get(0)).collect::<Vec<_>>(),
                   vec![Posting::new(0)]);
    }

    #[test]
    fn chunk_overflowing_inverting() {
        let (sorted_tx, sorted_rx) = mpsc::sync_channel(64);
        let result = thread::spawn(|| super::invert_index(sorted_rx, RamStorage::new()));

        sorted_tx.send((0..10)
                .map(|i| (i, (i..i + 100).map(|k| Posting::new(k)).collect::<Vec<_>>()))
                .collect::<Vec<_>>())
            .unwrap();
        drop(sorted_tx);

        let chunked_storage = result.join().unwrap().unwrap();
        assert_eq!(chunked_storage.len(), 10);
        assert_eq!(PostingDecoder::new(chunked_storage.get(0)).collect::<Vec<_>>(),
                   (0..100).map(|k| Posting::new(k)).collect::<Vec<_>>());

    }

    #[test]
    fn overflowing_posting() {
        let (sorted_tx, sorted_rx) = mpsc::sync_channel(64);
        let result = thread::spawn(|| super::invert_index(sorted_rx, RamStorage::new()));

        sorted_tx.send((0..1)
                .map(|i| (i, (i..i + 1).map(|k| Posting::new(k)).collect::<Vec<_>>()))
                .collect::<Vec<_>>())
            .unwrap();
        drop(sorted_tx);


        let chunked_storage = result.join().unwrap().unwrap();
        assert_eq!(chunked_storage.len(), 1);
        assert_eq!(PostingDecoder::new(chunked_storage.get(0)).collect::<Vec<_>>(),
                   (0..1).map(|k| Posting::new(k)).collect::<Vec<_>>());
    }


    #[test]
    fn write_listing_basic() {
        let mut storage = ChunkedStorage::new(10, Box::new(RamStorage::new()));
        let listing = vec![Posting::new(0), Posting::new(1)];
        {
            let mut chunk = storage.new_chunk(0);
            super::write_listing(listing, 0, &mut chunk).unwrap();
        }
        let ch = storage.get(0);
        let data = VByteDecoder::new(ch).collect::<Vec<_>>();
        assert_eq!(data, vec![0, 1]);
    }    
}
