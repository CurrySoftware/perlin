#![feature(test)]
extern crate rand;
extern crate test;
extern crate perlin;
#[macro_use]
extern crate lazy_static;

mod utils;

use utils::*;

use perlin::index::boolean_index::*;

lazy_static!{
    pub static ref INDICES: [BooleanIndex<usize>; 7] = [
        prepare_index(10, 10),      //100
        prepare_index(100, 100),    //10.000
        prepare_index(1000, 100),   //100.000
        prepare_index(10000, 100),  //1.000.000
        prepare_index(100000, 100), //10.000.000
        prepare_index(10000, 1000), //10.000.000
        prepare_index(10000, 10000),//100.000.000
    ];
}

macro_rules! bench {
    ($query:expr) =>
    {
        #[bench]
        fn i_10_10(b: &mut Bencher) {
            b.iter(||
                INDICES[0].execute_query($query).count()
            );
        }

        #[bench]
        fn i_100_100(b: &mut Bencher) {
            b.iter(||
                INDICES[1].execute_query($query).count()
            );
        }

        #[bench]
        fn i_1000_100(b: &mut Bencher) {
            b.iter(||
                INDICES[2].execute_query($query).count()
            );
        }

        #[bench]
        fn i_10000_100(b: &mut Bencher) {
            b.iter(||
                INDICES[3].execute_query($query).count()
            );
        }

        #[bench]
        fn i_100000_100(b: &mut Bencher) {
            b.iter(||
                INDICES[4].execute_query($query).count()
            );
        }

        #[bench]
        fn i_10000_1000(b: &mut Bencher) {
            b.iter(||
                INDICES[5].execute_query($query).count()
            );
        }

        #[bench]
        fn i_10000_10000(b: &mut Bencher) {
            b.iter(||
                INDICES[6].execute_query($query).count()
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
            bench!{$query}
            
        }

    )

}

build_bench!(atom_frequent, &BooleanQuery::Atom(QueryAtom::new(0, 4)));
build_bench!(atom_seldom, &BooleanQuery::Atom(QueryAtom::new(0, 10000)));
build_bench!(and_freq_freq, &BooleanQuery::NAry(BooleanOperator::And,
                                                vec![BooleanQuery::Atom(QueryAtom::new(0, 10)),
                                                     BooleanQuery::Atom(QueryAtom::new(0, 15))],
                                                None));
build_bench!(nested_and, &BooleanQuery::NAry(BooleanOperator::And,
                                             vec![BooleanQuery::NAry(BooleanOperator::And,
                                                                     vec![BooleanQuery::Atom(QueryAtom::new(0, 100)), BooleanQuery::Atom(QueryAtom::new(0, 200)), BooleanQuery::Atom(QueryAtom::new(0, 300))],
                                                                     None),
                                                  BooleanQuery::Atom(QueryAtom::new(0, 40))],
                                             None));
                                                                         
                                                                                             

                                                     


