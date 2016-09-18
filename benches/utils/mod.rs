use perlin::index::boolean_index::{IndexBuilder, BooleanIndex};
use perlin::index::storage::RamStorage;
use rand;

pub fn prepare_index(documents: usize, document_size: usize) -> BooleanIndex<usize> {
    let rng = ZipfGenerator::new(voc_size(45, 0.5, documents*document_size));
    let index = IndexBuilder::<_, RamStorage<_>>::new()
        .create((0..documents).map(|_| rng.take(document_size)));
    index.unwrap()
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
