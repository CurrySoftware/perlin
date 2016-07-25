use perlin::index::{Index, PersistentIndex};
use std::fmt::Debug;
use std::env;
use std::fs::File;
use rand;

pub fn prepare_index<'a, TIndex: Index<'a, usize> + PersistentIndex>(documents: usize,
                                                                     document_size: usize)
                                                                             -> TIndex {

  //  println!("Preparing Index with {} documents and {} terms per document", documents, document_size);
    let mut tmp_dir = env::temp_dir();
    tmp_dir.push(&format!("bench_index_{}_{}.bin", documents, document_size));

    if tmp_dir.exists() {
    //    println!("Found index in temporary directory. Reading it!");
        let result = TIndex::read_from(&mut File::open(tmp_dir.as_path()).unwrap()).unwrap();
    //    println!("Finished!");
        result
    } else {
        let rng = ZipfGenerator::new(voc_size(20, 0.5, documents * document_size));
    //    println!("Index has to be created. Starting...");
        let mut index = TIndex::new();
        let mut docs = Vec::with_capacity(documents);
        for i in 0..documents {
            if i % 1000 == 0
            {                
                println!("{}/{} documents", i, documents);
            }
            docs.push(rng.take(document_size));
        }
        index.index_documents(docs);
      //  println!("Finished creating the Index! Writing to Disk!");
        index.write_to(&mut File::create(tmp_dir.as_path()).unwrap()).unwrap();
     //   println!("Finished!");
        index
    }
}

// Implementation of Heaps' Law
fn voc_size(k: usize, b: f64, tokens: usize) -> usize {
    ((k as f64) * (tokens as f64).powf(b)) as usize
}

#[derive(Clone)]
struct ZipfGenerator {
    voc_size: usize,
    factor: f64,
    acc_probs: Box<Vec<f64>>,
}

impl ZipfGenerator {
    fn new(voc_size: usize) -> Self {
        let mut res = ZipfGenerator {
            voc_size: voc_size,
            factor: (1.78 * voc_size as f64).ln(),
            acc_probs: Box::new(Vec::with_capacity(voc_size)),
        };
        let mut acc = 0.0;
        for i in 1..voc_size {
            acc += 1.0 / (i as f64 * res.factor);
            res.acc_probs.push(acc);
        }
        res
    }
}

impl<'a> Iterator for &'a ZipfGenerator {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        let dice = rand::random::<f64>();
        let mut c = 0;
        loop {
            if dice < self.acc_probs[c] {
                return Some(c);
            }
            c += 1;
        }
    }
}
