use perlin::index::{Index, PersistentIndex};
use std::rc::Rc;
use std::fmt::Debug;
use std::env;
use std::path::Path;
use std::fs::File;
use rand::{StdRng, Rng};
use rand::Rand;
use rand;

pub fn prepare_index<TIndex: Index<usize> + Debug + PersistentIndex>(documents: usize,
                                                                     document_size: usize)
                                                                     -> TIndex {
    let mut tmp_dir = env::temp_dir();
    tmp_dir.push(&format!("bench_index_{}_{}.bin", documents, document_size));

    if tmp_dir.exists() {
        TIndex::read_from_file(tmp_dir.as_path())
    } else {
        let mut index = TIndex::new();
        let mut rng = ZipfGenerator::new(voc_size(50, 0.5, documents * document_size));
        for _ in 0..documents {
            index.index_document(rng.clone().take(document_size));
        }
        index.write_to_file(tmp_dir.as_path());
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

impl Iterator for ZipfGenerator {
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
