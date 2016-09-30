#![feature(test)]
extern crate rand;
extern crate test;
extern crate perlin;
#[macro_use]
extern crate lazy_static;

mod utils;

use utils::*;
use test::Bencher;
use perlin::index::boolean_index::IndexBuilder;
use perlin::storage::RamStorage;
use rand::{XorShiftRng, Generator, Rng};

lazy_static!{
    pub static ref COLLECTIONS: [Vec<Vec<usize>>; 4] = [
        (0..100).map(|_| ZipfGenerator::new(voc_size(45, 0.5, 12500)).take(125).collect()).collect(),//100kb
        (0..1000).map(|_| ZipfGenerator::new(voc_size(45, 0.5, 125000)).take(125).collect()).collect(),//1MB
        (0..10000).map(|_| ZipfGenerator::new(voc_size(45, 0.5, 1_250_000)).take(125).collect()).collect(),//10MB
        (0..100000).map(|_| ZipfGenerator::new(voc_size(45, 0.5, 1_500_000)).take(125).collect()).collect(),//100MB
    ];

}

#[bench]
fn index_100mb(b: &mut Bencher) {
    b.iter(|| {
        test::black_box(&IndexBuilder::<_, RamStorage<_>>::new().create(COLLECTIONS[3].iter().map(|i| i.iter())));
    });
}

#[bench]
fn index_10mb(b: &mut Bencher) {
    b.iter(|| {
        test::black_box(&IndexBuilder::<_, RamStorage<_>>::new().create(COLLECTIONS[2].iter().map(|i| i.iter())));
    });
}


#[bench]
fn index_1mb(b: &mut Bencher) {
    b.iter(|| {
        test::black_box(&IndexBuilder::<usize, RamStorage<_>>::new()
            .create(COLLECTIONS[1].iter().map(|i| i.iter().cloned())));
    });
}

#[bench]
fn index_100kb(b: &mut Bencher) {
    b.iter(|| {
        test::black_box(&IndexBuilder::<usize, RamStorage<_>>::new()
            .create(COLLECTIONS[0].iter().map(|i| i.iter().cloned())));

    });
}
