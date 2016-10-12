use perlin::index::boolean_index::{IndexBuilder, BooleanIndex};
use perlin::storage::CompressedRamStorage;
use rand;
use rand::{XorShiftRng, Rng};

#[allow(dead_code)]
pub fn prepare_index(documents: usize, document_size: usize) -> BooleanIndex<usize> {    
    let collection: Vec<Vec<usize>> = (0..documents)
        .map(|_| ZipfGenerator::new(voc_size(45, 0.5, documents * document_size)).take(document_size).collect())
        .collect();
    println!("Preparing index with {} documents each {} terms",
             documents,
             document_size);
    let index = IndexBuilder::<_, CompressedRamStorage<_>>::new()
        .create(collection.iter().map(|i| i.iter().cloned()));
    index.unwrap()
}

// Implementation of Heaps' Law
pub fn voc_size(k: usize, b: f64, tokens: usize) -> usize {
    ((k as f64) * (tokens as f64).powf(b)) as usize
}


#[derive(Clone)]
pub struct ZipfGenerator {
    voc_size: usize,
    factor: f32,
    acc_probs: Vec<f32>,
    rng: XorShiftRng,
}

impl ZipfGenerator {
    pub fn new(voc_size: usize) -> Self {
        let mut res = ZipfGenerator {
            voc_size: voc_size,
            factor: (1.78 * voc_size as f32).ln(),
            acc_probs: Vec::with_capacity(voc_size),
            rng: rand::weak_rng(),
        };
        let mut acc = 0.0;
        for i in 1..voc_size {
            acc += 1.0 / (i as f32 * res.factor);
            res.acc_probs.push(acc);
        }
        res.acc_probs.push(1f32);
        res
    }
}

impl<'a> Iterator for &'a mut ZipfGenerator {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        let dice = self.rng.next_f32();
        let result = match self.acc_probs.binary_search_by(|v| v.partial_cmp(&dice).unwrap()) {
            Ok(index) | Err(index) => index
        };
        Some(result)
    }
}
