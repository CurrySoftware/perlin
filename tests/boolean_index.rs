extern crate perlin;

use perlin::index::Index;
use perlin::index::boolean_index::*;
use perlin::index::storage::RamStorage;
use perlin::index::storage::FsStorage;

#[test]
fn build_and_query_persistent_index() {
    let doc1 = vec![0, 5, 10, 15, 20];
    let doc2 = vec![0, 7, 14, 21, 28];
    let doc3 = vec![0, 3, 6, 9, 12];
    let doc4 = vec![0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20];

    let folder = std::env::temp_dir().join("bq_persistent_test");

    assert!(std::fs::create_dir_all(folder.as_path()).is_ok());

    let index = IndexBuilder::<_, FsStorage<_>>::new()
        .persist(folder.as_path())
        .create_persistent(vec![doc1.into_iter(),
                                doc2.into_iter(),
                                doc3.into_iter(),
                                doc4.into_iter()]
            .into_iter())
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

    let index =
        IndexBuilder::<usize, FsStorage<_>>::new().persist(folder.as_path()).load().unwrap();

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

#[derive(PartialOrd, Ord, PartialEq, Eq)]
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
