//! This example will introduce you to text retrieval with perlin.
//! You will learn how to
//! * build indices
//! * index documents
//! * query the index for documents
//!
//! Let's get going!
//!
//! Setting:
//! We have ten documents in the collection folder.
//! Our mission is to index these ten documents and then provide simple search
//! interface to the user via commandline
//!

extern crate walkdir;
extern crate time;
extern crate perlin;

use std::path::Path;
use std::env;
use std::io::{stdin, Read};
use std::fs::File;
use std::ffi::OsString;

use walkdir::WalkDir;
use time::PreciseTime;

use perlin::index::storage::RamStorage;
use perlin::index::Index;
use perlin::index::boolean_index::{BooleanIndex, IndexBuilder, QueryBuilder};
use perlin::language::basic_analyzer;


fn main() {
    let args = env::args().collect::<Vec<_>>();
    if args.len() < 2 {
        print_usage();
        return;
    }
    // First lets get our documents loaded into RAM.
    let collection = load_collection(Path::new(&args[1]));
    // Now we will use the `IndexBuilder` to create the index
    let index = IndexBuilder::
    //The Index Builder is generic over two types:
    //The type of term the index will be handeling (in our case this is String)
    // The term type can be inferred and thus a simple `_` suffices.
    // And the type of the Storage the index will be using to store its data
    // which can not be inferred and has to be specified
    // In our case the collection is tiny. So we can simply keep it in RAM
    // Have a look at perlin::index::storage::FsStorage for a compressed storage on the file system
    <_, RamStorage<_>>::new()
    // Calling the create method will index the passed collection
    // It expects an iterator that iterates over the terms of each document.
    // Currently we only have a Vec<String>
    // What we want though is a Iterator<Item=Iterator<Item=String>>
    // Lets use the basic_analyzer() Method to turn each document into a Vector of terms
    // Then lets build iterators from that
        .create(collection.clone().into_iter().map(|doc| basic_analyzer(doc.as_str()).into_iter())).unwrap();
    // This approach may seem a bit verbose but it allows to sequentially index
    // documents which would enable indexing collections that dont fit into RAM


    // So now we have our index. Lets ask the user for a query
    println!("{} documents indexed. Ready to run your query. Type '?' for help!", collection.len());
    let mut input = String::new();
    while let Ok(_) = stdin().read_line(&mut input) {
        {let trimmed = input.trim();
        match trimmed {
            "?" => print_cli_usage(),
            _ => handle_input(&trimmed, &index, &collection)
        }
        }
        input.clear();
    }
}


fn handle_input(input: &str, index: &BooleanIndex<String>, docs: &Vec<String>) {
    println!("Querying Index for '{}'", input);
    // Run the query through the same analyzer as the documents
    // Then build an AND-Query from it
    let query = QueryBuilder::and(QueryBuilder::atoms(basic_analyzer(input))).build();
    // Retrieve the result. The index returns an iterator over the matching document-IDs
    // The query-execution process is lazy. Keep that in mind when working with huge collections.
    // It might help
    let start = PreciseTime::now();
    let query_result = index.execute_query(&query).collect::<Vec<_>>();
    let end = PreciseTime::now();
    println!("{} documents found in {}µs:", query_result.len(), start.to(end).num_microseconds().unwrap_or(0));
    for i in query_result {
        print!("{}:\t{}", i, docs[i as usize]);
    }    
}

fn print_cli_usage() {
    println!("Just type whatever words you are interested in. I.e. 'seventh planet'");
}

fn print_usage() {
    println!("Usage: text-retrieval path/to/collection/folder");
    println!("When running with cargo: cargo build --example text-retrieval examples/text-retrieval/collection");
}

fn load_collection(path: &Path) -> Vec<String> {
    let mut result = Vec::new();
    // Create an iterator over all entries in `path`
    let mut walker = WalkDir::new(path).into_iter();
    // Iterate over these entries
    while let Some(Ok(entry)) = walker.next() {
        // Only index files that end in ".doc"
        if !has_extension(entry.path(), "doc") {
            continue;
        }
        // Open file
        if let Ok(mut file) = File::open(entry.path()) {
            let mut s = String::new();
            // Read its contents into `s`
            if let Ok(_) = file.read_to_string(&mut s) {
                // add `s` to result
                result.push(s);
            }
        }
    }
    result
}

/// Little utility function
fn has_extension(path: &Path, extension: &str) -> bool {
    path.extension().unwrap_or(&OsString::new().as_os_str()).to_str() == Some(extension)
}
