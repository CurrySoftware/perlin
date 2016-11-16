//These benchmarks should currently not be considered complete, extensive or significant.
//Also indexing is currently too slow. So using them is not really fun.
#![feature(test)]
extern crate rand;
extern crate test;
extern crate perlin;
#[macro_use]
extern crate lazy_static;

mod utils;

use utils::*;

use perlin::index::boolean_index::BooleanIndex;

lazy_static!{
    pub static ref INDICES: [BooleanIndex<usize>; 3] = [
        prepare_index(1_000, 500), //1.000 documents with 500 terms a 8bytes = 4MB
        prepare_index(10_000, 500), //10.000 documents with 500 terms a 8bytes = 40MB
        prepare_index(1_000, 5000), //100.000 documents with 1000 terms a 8bytes = 800MB
    ];
}

macro_rules! bench {
    ($query:expr) =>
    {

        #[bench]
        fn typical_first_ten(b: &mut Bencher) {
            b.iter(||
                black_box(INDICES[0].execute_query($query).take(10).count())
            );
        }

        #[bench]
        fn large_collection_first_ten(b: &mut Bencher) {
            b.iter(||
                black_box(INDICES[1].execute_query($query).take(10).count())
            );
        }

        #[bench]
        fn large_documents_first_ten(b: &mut Bencher) {
            b.iter(||
                black_box(INDICES[2].execute_query($query).take(10).count())
            );
        }

        #[bench]
        fn typical(b: &mut Bencher) {
            b.iter(||
                black_box(INDICES[0].execute_query($query).count())
            );
        }

        #[bench]
        fn large_collection(b: &mut Bencher) {
            b.iter(||
                black_box(INDICES[1].execute_query($query).count())
            );
        }

        #[bench]
        fn large_documents_collection(b: &mut Bencher) {
            b.iter(||
                black_box(INDICES[2].execute_query($query).count())
            );
        }
    }
}

macro_rules! build_bench {
    ($name:ident, $query:expr) => (
        mod $name {
            use super::*;
            use test::Bencher;
            use perlin::index::boolean_index::*;
            use perlin::index::Index;
            use test::black_box;
            bench!{$query}
        }

    )

}

build_bench!(and_seldom_seldom, &QueryBuilder::and(QueryBuilder::atoms(vec![4000, 4002])).build());
build_bench!(and_frequent_seldom, &QueryBuilder::and(QueryBuilder::atoms(vec![4, 4002])).build());
build_bench!(and_frequent_frequent, &QueryBuilder::and(QueryBuilder::atoms(vec![4, 5])).build());
build_bench!(atom_frequent, &QueryBuilder::atom(4).build());
build_bench!(atom_seldom, &QueryBuilder::atom(4000).build());
