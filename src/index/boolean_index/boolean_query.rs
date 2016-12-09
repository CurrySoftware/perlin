//This concept was based on the idea, the positional queries can only be run on query atoms
//This is not the case anymore.
//TODO: Rethink!

/// Basic boolean operator. Use it in combination with a `BooleanQuery`
#[derive(Copy ,Clone)]
pub enum BooleanOperator {
    Or,
    And,
}

/// Basic filter operator. Use it in combination with a `BooleanQuery`
#[derive(Copy, Clone)]
pub enum FilterOperator {
    Not,
}

/// Basic positional operator. Use it in combination with a `BooleanQuery`
#[derive(Copy, Clone)]
pub enum PositionalOperator {
    /// Ensures that QueryAtoms are in the specified order and placement
    /// See `BooleanQuery::Positional` for more information
    InOrder,
}

/// Stores term to be compared against and relative position of a query atom
pub struct QueryAtom<TTerm> {
    pub relative_position: usize,
    pub query_term: TTerm,
}

impl<TTerm> QueryAtom<TTerm> {
    pub fn new(relative_position: usize, query_term: TTerm) -> Self {
        QueryAtom {
            relative_position: relative_position,
            query_term: query_term,
        }
    }
}


pub enum BooleanQuery<TTerm> {
    Atom(QueryAtom<TTerm>),

    // Different from NAry because positional queries can currently only run on query-atoms.
    // To ensure correct usage, this rather inelegant abstraction was implemented
    // Nevertheless, internally both are handled by the same code
    // See `NAryQueryIterator::new` and `NAryQueryIterator::new_positional`
    Positional(PositionalOperator, Vec<QueryAtom<TTerm>>),
    NAry(BooleanOperator, Vec<BooleanQuery<TTerm>>),
    Filter(FilterOperator,
           // sand
           Box<BooleanQuery<TTerm>>,
           // sieve
           Box<BooleanQuery<TTerm>>),
}
