#![feature(test)]
extern crate rand;
extern crate test;
extern crate perlin;

mod utils;

use utils::*;

use test::Bencher;

use perlin::index::boolean_index::*;
use perlin::index::Index;


macro_rules! atom_query {
    ($name:ident, $docs:expr, $terms:expr) => {
        #[bench]
        fn atom_query$name(b: &mut Bencher) {
            let index: BooleanIndex<usize> = prepare_index($docs, $terms);
            b.iter(|| {
                index.execute_query(&BooleanQuery::Atom(QueryAtom::new(0, 4)));
            });
        }

    }
}

fn atom_query_1010() {

}

atom_query!(a, 10, 10);
atom_query!(b, 50, 50);
atom_query!(c, 100, 100);

