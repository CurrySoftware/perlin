use index::boolean_index::boolean_query::{QueryAtom, BooleanOperator, PositionalOperator,
                                          FilterOperator, BooleanQuery};

/// This struct provides a flexible and ergonomic was to build `BooleanQueries`
///
/// # Examples
/// ## Simple Atom-Queries
/// Querying an Atom:
///
/// ```rust
/// use perlin::index::boolean_index::QueryBuilder;
/// //Would return all document ids of documents that contain '4'
/// let query = QueryBuilder::atom(4).build();
/// ```
/// ## Boolean Operators
/// Querying for two Atoms:
///
/// ```rust
/// use perlin::index::boolean_index::QueryBuilder;
/// //Would return all document ids of documents that contain '4' AND '8'
/// let and_query = QueryBuilder::and(QueryBuilder::atoms(vec![4, 8])).build();
///
/// //Would return all document ids of documents that contain '4' OR '8'
/// let or_query = QueryBuilder::or(QueryBuilder::atoms(vec![4, 8])).build();
/// ```
///
/// ## Positional Operators
/// Querying for phrases:
///
/// ```rust
/// use perlin::index::boolean_index::QueryBuilder;
///
/// //Would return all document ids of documents that contain '1 2 3'
/// let phrase_query = QueryBuilder::in_order(
///                vec![Some(1), Some(2), Some(3)]).build();
///
/// //Would match any phrase in following form "this is a * house"
/// //Where '*' can be any term
/// //i.e. "this is a blue house" or "this is a small house"
/// //It would not match "this is a house" though.
/// let placeholder_query = QueryBuilder::in_order(
///     vec![Some("this"),Some("is"), Some("a"), None, Some("house")]).build();
/// ```
/// ## Query Filters
///
/// ```rust
/// use perlin::index::boolean_index::QueryBuilder;
///
/// //Would return all document ids of documents that contain '4' but NOT '8'
/// let filtered_query =
/// QueryBuilder::atom(4).not(QueryBuilder::atom(8)).build();
/// ```
///
/// ## Nested Queries
/// Query objects can be nested in arbitrary depth:
///
/// ```rust
/// use perlin::index::boolean_index::QueryBuilder;
///
/// let nested_query = QueryBuilder::and(vec![
///                         QueryBuilder::or(QueryBuilder::atoms(vec![0, 4])),
///                         QueryBuilder::in_order(
///                                    vec![Some(9),
///                                         Some(7)])
///                                    .not(QueryBuilder::atom(3))]).build();
/// ```


pub struct QueryBuilder<TTerm> {
    query: BooleanQuery<TTerm>,
}

impl<TTerm> QueryBuilder<TTerm> {
    /// Operands are connected by the AND-Operator. All operands have to occur
    /// in a document for it to match
    pub fn and(operands: Vec<QueryBuilder<TTerm>>) -> Self {
        QueryBuilder {
            query: BooleanQuery::NAry(BooleanOperator::And,
                                      operands.into_iter().map(|o| o.build()).collect::<Vec<_>>()),
        }
    }

    /// Operands are connected by the OR-Operator. Any operand can occur in a docment for it to match
    pub fn or(operands: Vec<QueryBuilder<TTerm>>) -> Self {
        QueryBuilder {
            query: BooleanQuery::NAry(BooleanOperator::Or,
                                      operands.into_iter().map(|o| o.build()).collect::<Vec<_>>()),
        }
    }

    /// Turns a vector of terms into a vector of `QueryBuilder` objects.
    ///
    /// Useful utility function to be used with other methods in this struct.
    /// See module level documentation for examples.
    pub fn atoms(terms: Vec<TTerm>) -> Vec<Self> {
        terms.into_iter().map(QueryBuilder::atom).collect::<Vec<_>>()
    }

    /// Most simple query for just a term. Documents containing the term match, all others do not.
    pub fn atom(term: TTerm) -> Self {
        QueryBuilder { query: BooleanQuery::Atom(QueryAtom::new(0, term)) }
    }

    /// Use this method to build phrase queries.
    ///
    /// Operands must occur in the same order as passed to the method in the document.
    /// Operands are wrapped inside `Option` to allow for placeholders in phrase queries.
    ///
    /// I.e. Some(A):  A has to occur at that position
    ///      None: Any term can occur at that position.
    /// See module level documentation for examples
    pub fn in_order(operands: Vec<Option<TTerm>>) -> Self {
        QueryBuilder {
            query: BooleanQuery::Positional(PositionalOperator::InOrder,
                                            operands.into_iter()
                                                .enumerate()
                                                .filter(|&(_, ref t)| t.is_some())
                                                .map(|(i, t)| QueryAtom::new(i, t.unwrap()))
                                                .collect::<Vec<_>>()),
        }
    }


    /// Applies the NOT operator between to queries.
    /// E.g. returns all document ids that match self and NOT filter
    /// See module level documentation for example
    pub fn not(self, filter: Self) -> Self {
        QueryBuilder {
            query: BooleanQuery::Filter(FilterOperator::Not,
                                        Box::new(self.build()),
                                        Box::new(filter.build())),
        }

    }

    /// Final method to be called in the query building process.
    /// Returns the actual `BooleanQuery` object to be passed to the index.
    pub fn build(self) -> BooleanQuery<TTerm> {
        self.query
    }
}


#[cfg(test)]
mod tests {
    use super::QueryBuilder;

    use index::Index;
    use index::boolean_index::tests::prepare_index;

    #[test]
    fn and_query() {
        let index = prepare_index();
        let query = QueryBuilder::and(QueryBuilder::atoms(vec![0, 5])).build();
        assert_eq!(index.execute_query(&query).collect::<Vec<_>>(), vec![0, 2]);
    }

    #[test]
    fn nested_and() {
        let index = prepare_index();
        let query = QueryBuilder::and(vec![QueryBuilder::or(QueryBuilder::atoms(vec![12, 7])),
                                           QueryBuilder::atom(9)])
            .build();
        assert_eq!(index.execute_query(&query).collect::<Vec<_>>(), vec![0]);
    }


    #[test]
    fn not() {
        let index = prepare_index();
        let query =
            QueryBuilder::and(QueryBuilder::atoms(vec![0, 5])).not(QueryBuilder::atom(9)).build();
        assert_eq!(index.execute_query(&query).collect::<Vec<_>>(), vec![2]);
    }

    #[test]
    fn in_order() {
        let index = prepare_index();
        let query = QueryBuilder::in_order(vec![Some(0), None, Some(2)]).build();
        assert_eq!(index.execute_query(&query).collect::<Vec<_>>(), vec![0]);
        let query = QueryBuilder::in_order(vec![Some(0), Some(2)]).build();
        assert_eq!(index.execute_query(&query).collect::<Vec<_>>(), vec![1]);
    }
}
