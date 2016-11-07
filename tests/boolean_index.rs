extern crate rand;
extern crate perlin;

use perlin::index::Index;
use perlin::index::boolean_index::*;
use perlin::storage::RamStorage;
use perlin::storage::FsStorage;

#[test]
fn build_and_query_persistent_index() {
    let doc1 = vec![0, 5, 10, 15, 20];
    let doc2 = vec![0, 7, 14, 21, 28];
    let doc3 = vec![0, 3, 6, 9, 12];
    let doc4 = vec![0 as u32, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20];

    let folder = std::env::temp_dir().join("bq_persistent_test");

    assert!(std::fs::create_dir_all(folder.as_path()).is_ok());

    let index = IndexBuilder::<u32, FsStorage<_>>::new()
        .persist(folder.as_path())
        .create_persistent(vec![doc1.into_iter(), doc2.into_iter(), doc3.into_iter(), doc4.into_iter()].into_iter())
        .unwrap();

    let pos_query = QueryBuilder::in_order(vec![Some(10), Some(20)]).build();
    assert_eq!(index.execute_query(&pos_query).collect::<Vec<_>>(), vec![]);

    let pos_query = QueryBuilder::in_order(vec![Some(0), Some(7), Some(14)]).build();
    assert_eq!(index.execute_query(&pos_query).collect::<Vec<_>>(), vec![1]);

    let and_query = QueryBuilder::and(QueryBuilder::atoms(vec![0, 6, 12])).build();
    assert_eq!(index.execute_query(&and_query).collect::<Vec<_>>(),
               vec![2, 3]);

    let or_query = QueryBuilder::or(QueryBuilder::atoms(vec![0, 6, 12])).build();
    assert_eq!(index.execute_query(&or_query).collect::<Vec<_>>(),
               vec![0, 1, 2, 3]);

    let index = IndexBuilder::<usize, FsStorage<_>>::new().persist(folder.as_path()).load().unwrap();

    let pos_query = QueryBuilder::in_order(vec![Some(10), Some(20)]).build();
    assert_eq!(index.execute_query(&pos_query).collect::<Vec<_>>(), vec![]);

    let pos_query = QueryBuilder::in_order(vec![Some(0), Some(7), Some(14)]).build();
    assert_eq!(index.execute_query(&pos_query).collect::<Vec<_>>(), vec![1]);

    let and_query = QueryBuilder::and(QueryBuilder::atoms(vec![0, 6, 12])).build();
    assert_eq!(index.execute_query(&and_query).collect::<Vec<_>>(),
               vec![2, 3]);

    let or_query = QueryBuilder::or(QueryBuilder::atoms(vec![0, 6, 12])).build();
    assert_eq!(index.execute_query(&or_query).collect::<Vec<_>>(),
               vec![0, 1, 2, 3]);
}

#[derive(Hash, PartialOrd, Ord, PartialEq, Eq)]
enum Color {
    Red,
    Green,
    Blue,
}

#[test]
fn build_and_query_volatile_index() {
    let doc1 = vec![Color::Red, Color::Red, Color::Blue];
    let doc2 = vec![Color::Green, Color::Red, Color::Blue];
    let doc3 = vec![Color::Blue, Color::Blue, Color::Blue];

    let index = IndexBuilder::<_, RamStorage<_>>::new()
        .create(vec![doc1.into_iter(), doc2.into_iter(), doc3.into_iter()].into_iter())
        .unwrap();

    let pos_query = QueryBuilder::in_order(vec![Some(Color::Red), Some(Color::Blue)]).build();
    assert_eq!(index.execute_query(&pos_query).collect::<Vec<_>>(),
               vec![0, 1]);

    let pos_query = QueryBuilder::in_order(vec![Some(Color::Red), Some(Color::Red), Some(Color::Blue)]).build();
    assert_eq!(index.execute_query(&pos_query).collect::<Vec<_>>(), vec![0]);

    let and_query = QueryBuilder::and(QueryBuilder::atoms(vec![Color::Green, Color::Blue])).build();
    assert_eq!(index.execute_query(&and_query).collect::<Vec<_>>(), vec![1]);

    let or_query = QueryBuilder::or(QueryBuilder::atoms(vec![Color::Green, Color::Blue])).build();
    assert_eq!(index.execute_query(&or_query).collect::<Vec<_>>(),
               vec![0, 1, 2]);
}


mod random_data {

    use perlin::index::boolean_index::{QueryBuilder, IndexBuilder, BooleanIndex};
    use perlin::storage::RamStorage;
    use perlin::index::Index;

    use rand;
    use rand::{XorShiftRng, Rng};

    #[allow(dead_code)]
    pub fn prepare_index(documents: usize, document_size: usize) -> BooleanIndex<usize> {
        println!("Building Collection!");
        println!("Voc size: {}", voc_size(45, 0.5, documents * document_size));
        let collection: Vec<Vec<usize>> = (0..documents)
            .map(|_| ZipfGenerator::new(voc_size(45, 0.5, documents * document_size)).take(document_size).collect())
            .collect();
        println!("Preparing index with {} documents each {} terms",
                 documents,
                 document_size);
        let index = IndexBuilder::<_, RamStorage<_>>::new()
            .create(collection.iter().map(|i| i.iter().cloned()).inspect(|i| println!("{:?}", i)));
        println!("Done!");
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
                Ok(index) | Err(index) => index,
            };
            Some(result)
        }
    }


    #[test]
    fn small_collection() {
        let index = prepare_index(10, 50);
        index.execute_query(&QueryBuilder::atom(4).build()).count();
    }

    #[test]
    fn large_collection() {
        let index = prepare_index(50, 50);
        index.execute_query(&QueryBuilder::atom(4).build()).count();
    }

    // #[test]
    // fn huge_collection() {
    //     let index = prepare_index(1000, 5000);
    //     index.execute_query(&QueryBuilder::atom(4).build()).count();
    // }

}
