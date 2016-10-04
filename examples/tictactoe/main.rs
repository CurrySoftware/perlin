//! This example will introduce you to custom-data retrieval with perlin
//! Perlin is basically data-type agnostic. Nevertheless, some traits need to
//! be implemented for certain features.
//!
//! This example will show you:
//! * what traits to implement to yield certain functionality
//! * how to implement these traits
//! * what is important to optimize these implementations
//!
//! Let's get going!
//!
//! Setting:
//! We have a collection of tic-tac-toe games and we want to be able to search
//! for certain board layouts or layout sequences.
//! Our mission is to index these tic-tac-toe games and provide a simple search
//! interface to the user via commandline.

extern crate perlin;

use perlin::index::Index;
use perlin::index::boolean_index::{QueryBuilder, IndexBuilder, BooleanIndex};
use perlin::storage::FsStorage;

// Lets think about tic-tac-toe games: we have two player who take turns
// populating a 3x3 field.
// The one who first has three fields in a row or diagonal wins. This is a very
// simple setup and a played game can easily be represented by the series of
// moves. We assume that the first player is assigned "x".
// For example: [(2,0), (0,0), (0,2), (1, 1), (2, 2), (2, 1), (1, 2)]
// represents the example game shown in the wikipedia:
// https://en.wikipedia.org/wiki/Tic-tac-toe#/media/File:Tic-tac-toe-game-1.svg
// Beside the moves we will also store the winner in our struct, because we do
// not want to implement game logic.
type TicTacToeGame = Vec<(u8, u8)>;

// When indexing text-based documents, these documents are split into terms to
// make the collection queryable.
// When working with custom types you have to ask yourself what your query
// terms should be. In our case it makes no sense to index tic-tac-toe games as
// a sequence of moves.
// We would need to know the complete game to be able to retrieve it.
// Rather, we would like to search our collection of games for certain
// layouts.
// For example: Show me all games that start with an "X" in the middle.
// Or: Show me all games where layout A leads to layout B.
// To achive this we must index the complete contents of a board after each
// move rather than
// the sequence of moves.

// For each field on the board we have three possible states: X, O or None
// To encode these states we need 2 bits per field, leaving us at 9*2bits =
// 18bits per move.
// This seems ok. But lets do the math: we have 3^9 states (9 fields with each
// 3 states). Thats 19683 possible states
// To encode 19683 we do not need 18 bits. log2(19683) = 14.3 bits.
// We encountered a typical problem: while it is convenient to encode the board
// as 9 fields with 2 bits each this solution lacks performance and preciseness.
// What we will do instead is encode every board layout as u16.
// Think of a u16 represented in tenery number. The last 9 positions of this
// number represent the states of the fields.
type BoardLayout = u16;


// What we now need is a converter that converts a TicTacToeGame into a series
// of BoardLayouts:
struct TicTacToeStateIterator {
    game: TicTacToeGame,
    last_layout: u16,
    pos: usize,
}

impl TicTacToeStateIterator {
    fn new(game: TicTacToeGame) -> Self {
        TicTacToeStateIterator {
            game: game,
            last_layout: 0,
            pos: 0,
        }
    }
}

// Lets number the fields on a tic tac toe game
// | 0 | 1 | 2 |
// | 3 | 4 | 5 |
// | 6 | 7 | 8 |
// So the number is x + y*3.
// Test: (1, 1) => 1 + 1*3 = 4.
//       (2, 0) => 2 + 0*3 = 2.
// For every move we will add 3^2 * player to self.last_layout
// For example: The first move is (2, 0).
// We will return 3^pos * player => 3^2 * 1 = 9
// The first board layout is 9 or in tenery 000000100.
// Encoding the second move (0, 0) gives us: 3^0 * 2 = 2
// Add it to the old board layout leves us with: 11 or 000000102
// And so on
impl Iterator for TicTacToeStateIterator {
    type Item = BoardLayout;

    fn next(&mut self) -> Option<Self::Item> {
        if self.pos < self.game.len() {
            // Determine field number of move
            let field: u32 = (self.game[self.pos].0 + self.game[self.pos].1 * 3) as u32;
            // Determine player number
            let player: u16 = ((self.pos % 2) + 1) as u16;
            // Calculate new board layout
            self.last_layout += ((3 as u16).pow(field) * player) as u16;
            self.pos += 1;
            // And return it
            Some(self.last_layout)
        } else {
            // Complete game iterated. No more boad layouts to return
            None
        }
    }
}

// We know how to represent the game and how to index it. Let go one step
// further.
// We want the index to be persisted on the filesystem. For that to be possible
// we have to tell the index how to encode and decode its terms. In our case
// BoardLayout or u16.
// This is done by implementing both perlin::storage::ByteDecodable and
// ByteEncodable
// Luckily for us, ByteDecodable and ByteEncodable are already implemented by
// perlin for u16.
// So there is nothing to do for us.




// Custom data-types in perlin also need to implement `Ord` and `Eq` but u16
// obviously does this.
// So we have prepared everything. Now we can handle tic-tac-toe games as
// easily as lets say text.
// Lets define some tictactoe games to work with. In a real application you
// would probably read them from a file or a database or something like that
// Credits go to https://xkcd.com/832/
fn main() {
    let games: Vec<TicTacToeGame> =
        vec![vec![(0, 0), (1, 0), (1, 1), (1, 2), (2, 2)],
             vec![(1, 2), (1, 1), (2, 2), (0, 2), (0, 1), (2, 0)],
             vec![(0, 0), (2, 2), (2, 0), (1, 0), (0, 2), (1, 1), (0, 1)],
             vec![(0, 2), (1, 1), (1, 2), (2, 2), (0, 0), (0, 1), (2, 0), (1, 0), (2, 1)]];

    let index = load_or_create_index(games.clone());

    println!("{} tictactoe-games indexed. Ready to run your query. Type '?' for help!",
             games.len());
    let mut input = String::new();
    while let Ok(_) = std::io::stdin().read_line(&mut input) {
        {
            let trimmed = input.trim();
            match trimmed {
                "?" => print_cli_usage(),
                _ => handle_input(trimmed, &index, &games),
            }
        }
        input.clear();
    }
}

fn handle_input(input: &str, index: &BooleanIndex<BoardLayout>, games: &[TicTacToeGame]) {
    // Split the input at whitespaces and try to parse them as numbers which represent layouts
    let layouts = input.split_whitespace()
        .map(|num| num.parse::<u16>())
        .filter(|num| num.is_ok())
        .map(|num| Some(num.unwrap()))       
        .collect::<Vec<_>>();

    //Build an phrase query from these layouts
    let query = QueryBuilder::in_order(layouts).build();
    //Retrieve the results
    let query_result = index.execute_query(&query).collect::<Vec<_>>();
    println!("{} games match!", query_result.len());
    for i in query_result{
        println!("{}:\t{:?}",i, games[i as usize]); 
    }
}

fn print_cli_usage() {
    println!("Find all the games with certain board layouts and board layout sequences.");
    println!("For example: '1' will yield all the games that start with x in the upper left \
              corner.");
    println!("'1 163' yields all the games that start with x in the upper left corner and are \
              countered by an o in the middle");
    println!("Have a look in the code for a detailed description of how games are encoded.");
    println!("(Of course this is rather unfriendly to use for the user. Sorry!)");
}

fn load_or_create_index(games: Vec<TicTacToeGame>) -> BooleanIndex<BoardLayout> {
    // We want to persist our index in the temporary folder of our OS. In my case
    // it is /tmp/.
    // So lets see if a tictactoe folder does exist:
    let index_dir = std::env::temp_dir().join("tictactoe");
    if index_dir.is_dir() {
        // The directory of the index exists. We assume we can load from that
        // directory. If not... no worries. We will just get an error back
        if let Ok(index) = IndexBuilder::<_, FsStorage<_>>::new().persist(&*index_dir).load() {
            println!("Index successfully loaded!");
            return index;
        }
    }
    // Directory does not exist or index could not be loaded
    // Create directory
    std::fs::create_dir_all(&index_dir).unwrap();
    // Build index
    IndexBuilder::<_, FsStorage<_>>::new()
        .persist(&*index_dir)
        .create_persistent(games.into_iter().map(TicTacToeStateIterator::new))
        .unwrap()
}
