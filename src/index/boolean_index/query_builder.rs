use index::boolean_index::boolean_query::{QueryAtom, BooleanOperator, PositionalOperator, FilterOperator,
                           BooleanQuery};

pub struct QueryBuilder<TTerm> {
    query: BooleanQuery<TTerm>,
}

impl<TTerm> QueryBuilder<TTerm> {
    pub fn and(operands: Vec<QueryBuilder<TTerm>>) -> Self {
        QueryBuilder {
            query: BooleanQuery::NAry(BooleanOperator::And,
                                      operands.into_iter().map(|o| o.build()).collect::<Vec<_>>()),
        }
    }

    pub fn or(operands: Vec<QueryBuilder<TTerm>>) -> Self {
        QueryBuilder {
            query: BooleanQuery::NAry(BooleanOperator::Or,
                                      operands.into_iter().map(|o| o.build()).collect::<Vec<_>>()),
        }
    }


    pub fn atoms(terms: Vec<TTerm>) -> Vec<Self> {
        terms.into_iter().map(QueryBuilder::atom).collect::<Vec<_>>()
    }


    pub fn atom(term: TTerm) -> Self {
        QueryBuilder { query: BooleanQuery::Atom(QueryAtom::new(0, term)) }
    }


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

    pub fn not(self, filter: Self) -> Self {
        QueryBuilder {
            query: BooleanQuery::Filter(FilterOperator::Not,
                                        Box::new(self.build()),
                                        Box::new(filter.build())),
        }

    }

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
