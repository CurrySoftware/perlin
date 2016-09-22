extern crate perlin;

use perlin::language::basic_analyzer;
use perlin::storage::RamStorage;
use perlin::index::boolean_index::{IndexBuilder, QueryBuilder};
use perlin::index::Index;

#[test]
fn main() {
    // The keeper database.
    // Source: "Inverted Files for Text Search Engines" by Justin Zobel and Alistair Moffat, July 2006
    let collection = vec!["The old night keeper keeps the keep in the town",
                          "In the big old house in the big old gown.",
                          "The house in the town had the big old keep",
                          "Where the old night keeper never did sleep.",
                          "The night keeper keeps the keep in the night",
                          "And keeps in the dark and sleeps in the light."];

    // Create the index in RAM
    let index = IndexBuilder::<_, RamStorage<_>>::new()
        .create(collection.iter().map(|doc| basic_analyzer(doc).into_iter()))
        .unwrap();

    // Build simple query for "keeper"
    let keeper_query = QueryBuilder::atom("keeper".to_string()).build();
    assert_eq!(index.execute_query(&keeper_query).collect::<Vec<_>>(),
               vec![0, 3, 4]);

    // Build phrase query for "old night keeper"
    let keeper_phrase =
        QueryBuilder::in_order(
            vec![
                Some("old".to_string()),
                Some("night".to_string()),
                Some("keeper".to_string())])
            .build();
    assert_eq!(index.execute_query(&keeper_phrase).collect::<Vec<_>>(),
               vec![0, 3]);

}
