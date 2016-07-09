#![feature(test)]
extern crate rand;
extern crate test;
extern crate perlin;
#[macro_use]
extern crate lazy_static;

mod utils;

use utils::*;

use test::Bencher;

use perlin::index::boolean_index::*;
use perlin::index::Index;

lazy_static!{
    static ref INDEX_10_10: BooleanIndex<usize> = prepare_index(10, 10);
    static ref INDEX_100_100: BooleanIndex<usize> = prepare_index(100, 100);
    static ref INDEX_1000_1000: BooleanIndex<usize> = prepare_index(1000, 1000);
    static ref INDEX_10000_1000: BooleanIndex<usize> = prepare_index(10000, 1000);
//    static ref INDEX_100000_1000: BooleanIndex<usize> = prepare_index(100000, 1000);
}

#[bench]
fn atom_query_frequent_10_10(b: &mut Bencher) {
    b.iter(|| {
        INDEX_10_10.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 4)));
    });
}

#[bench]
fn atom_query_frequent_100_100(b: &mut Bencher) {
    b.iter(|| {
        INDEX_100_100.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 4)));
    });
}

#[bench]
fn atom_query_frequent_1000_1000(b: &mut Bencher) {
    b.iter(|| {
        INDEX_1000_1000.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 4)));
    });
}


#[bench]
fn atom_query_frequent_10000_1000(b: &mut Bencher) {
    b.iter(|| {
      INDEX_10000_1000.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 4))).document_ids;
    });
}

#[bench]
fn atom_query_seldom_10_10(b: &mut Bencher) {
    b.iter(|| {
        INDEX_10_10.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 40)));
    });
}

#[bench]
fn atom_query_seldom_100_100(b: &mut Bencher) {
    b.iter(|| {
        INDEX_100_100.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 400)));
    });
}

#[bench]
fn atom_query_seldom_1000_1000(b: &mut Bencher) {
    b.iter(|| {
        INDEX_1000_1000.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 4000)));
    });
}


#[bench]
fn atom_query_seldom_10000_1000(b: &mut Bencher) {
    b.iter(|| {
      INDEX_10000_1000.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 400000))).document_ids;
    });
}

#[bench]
fn and_query_frequent_frequent_10000_1000(b: &mut Bencher) {
    b.iter(|| {
        INDEX_10000_1000.execute_query(&BooleanQuery::NAryQuery(BooleanOperator::And,
                                                                vec![BooleanQuery::Atom(QueryAtom::new(0, 10)),
                                                                     BooleanQuery::Atom(QueryAtom::new(0, 15))],
                                                                None));
    });    
}

#[bench]
fn and_query_frequent_seldom_10000_1000(b: &mut Bencher) {
    b.iter(|| {
        INDEX_10000_1000.execute_query(&BooleanQuery::NAryQuery(BooleanOperator::And,
                                                                vec![BooleanQuery::Atom(QueryAtom::new(0, 10)),
                                                                     BooleanQuery::Atom(QueryAtom::new(0, 40000))],
                                                                None));
    });    
}

#[bench]
fn and_query_seldom_seldom_10000_1000(b: &mut Bencher) {
    b.iter(|| {
        INDEX_10000_1000.execute_query(&BooleanQuery::NAryQuery(BooleanOperator::And,
                                                                vec![BooleanQuery::Atom(QueryAtom::new(0, 42300)),
                                                                     BooleanQuery::Atom(QueryAtom::new(0, 40000))],
                                                                None));
    });    
}


// #[bench]
// fn atom_query_100000_1000(b: &mut Bencher) {
//     b.iter(|| {
//         INDEX_100000_1000.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 4)));
//     });
// }
