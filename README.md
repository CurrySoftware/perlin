# perlin
A lazy, zero-allocation and data-agnostic Information Retrieval library

## Features

- Boolean Retrieval supporting arbitrary types and
  - Nested phrase queries with filters evaluating lazily and without allocations
  - being persistent on disk
  - being fast in RAM

## Dependencies

std

## Usage

```rust
extern crate perlin;

use perlin::language::basic_analyzer;
use perlin::storage::RamStorage;
use perlin::index::boolean_index::{IndexBuilder, QueryBuilder};
use perlin::index::Index;

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
}



```

See [documentation](https://doc.perlin-ir.org) or [examples](https://github.com/JDemler/perlin/tree/master/examples) for more.


## Current Status
Verison 0.1 marks the first state where this library might be useful to somebody. Nevertheless, there are still some issues with the current implementation:

- Loading indices from corrupted data does not yield good or useful errors
- Data in RAM is not compressed
- Indices are non mutable. Once they are create documents can not be added or removed
     
## Roadmap
In the long run this library will hopefully become a fully featured information retrieval library supporting modern ranked retrieval, natural language processing tool, facetted search and taxonomies.


