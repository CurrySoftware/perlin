# perlin
A lazy, zero-allocation and data-agnostic Information Retrieval library

## Features

- Boolean Retrieval supporting arbitrary types and
  - Nested phrase queries with filters evaluating lazily and without allocations
  - being persistent on disk
  - being fast in RAM

## Dependencies

std

## Current Status
Verison 0.1 marks the first state where this library might be useful to somebody. Nevertheless, there are still some issues with the current implementation:

- Indexing is incredibly slow 
- Loading indices from corrupted data does not yield good or useful errors
- Data in RAM is not compressed
- Indices are non mutable. Once they are create documents can not be added or removed
     
## Roadmap
In the long run this library will hopefully become a fully featured information retrieval library supporting modern ranked retrieval, natural language processing tool, facetted search and taxonomies.

## Usage
See [documentation](https://doc.perlin-ir.org) or [examples](https://github.com/JDemler/perlin/tree/master/examples).

