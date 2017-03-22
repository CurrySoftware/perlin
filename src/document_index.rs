use std::marker::PhantomData;

use query::{Query, Operand};
use perlin_core::index::posting::DocId;

pub type Pipeline<Out, T> = Box<Fn(DocId, &mut T, &str) -> PhantomData<Out> + Sync + Send>;
pub type QueryPipeline<T> = Box<for<'r> Fn(&'r T, Query<'r>) -> Operand<'r> + Sync + Send>;

#[cfg(test)]
mod tests {
    use perlin_core::index::posting::DocId;
    use field::Field;

    use rust_stemmers::Algorithm;

    use test_utils::create_test_dir;

    #[derive(PerlinDocument)]
    pub struct Test {
        text: Field<String>,
        title: Field<String>,
        #[no_pipe]
        #[filter]
        number: Field<u64>,
        #[no_pipe]
        emails: Field<usize>,
    }

    use language::{Stemmer, LowercaseFilter, WhitespaceTokenizer};
    use language::integers::NumberFilter;
    use std::borrow::Cow;
    use perlin_core::index::posting::Posting;
    use query::Query;


    fn create_and_fill_index(name: &str) -> TestIndex {
        let mut t = TestIndex::create(create_test_dir(name));
        t.set_text_pipeline(pipeline!(text
                           WhitespaceTokenizer
                           > NumberFilter
                             | [number]
                           > LowercaseFilter
                           > Stemmer(Algorithm::English)));
        t.set_title_pipeline(pipeline!(title
                                       WhitespaceTokenizer
                                       > LowercaseFilter
                                       > Stemmer(Algorithm::English)));
        t.set_query_pipeline(query_pipeline!(
            WhitespaceTokenizer
                > NumberFilter
                | Must [Any in number]
                > LowercaseFilter
                > Stemmer(Algorithm::English)
                > Must [All in text]
        ));
        t.add_document(&[(Cow::from("text"), Cow::from("10 birds flew over MT EVEREST"))]);
        t.add_document(&[(Cow::from("text"), Cow::from("125 birds flew accross THE ocean"))]);
        t.add_document(&[(Cow::from("title"), Cow::from("Unicorns on Deimos")),
                         (Cow::from("text"),
                          Cow::from("2567 unicorns flew from phobos to deimos"))]);
        t.commit();
        t
    }

    fn should_yield(index: &TestIndex, query: &str, ids: &[u32]) {
        if index.run_query(Query::new(query.to_string())).collect::<Vec<_>>() !=
           ids.iter().map(|id| Posting(DocId(*id))).collect::<Vec<_>>() {
            assert!(false,
                    format!("{} resulted in {:?} expexted {:?}",
                            query,
                            index.run_query(Query::new(query.to_string())).collect::<Vec<_>>(),
                            ids.iter().map(|id| Posting(DocId(*id))).collect::<Vec<_>>()))
        }
    }

    #[test]
    #[should_panic]
    fn negative_test() {
        let t = create_and_fill_index("doc_index/negative_test");
        should_yield(&t, "10 deimos", &[]);
        should_yield(&t, "2567 deimos", &[22]);
    }

    #[test]
    fn basic_test() {
        let t = create_and_fill_index("doc_index/basic_test");
        should_yield(&t, "10 deimos", &[]);
        should_yield(&t, "2567 deimos", &[2]);
    }

    #[test]
    fn empty_query() {
        let mut t = create_and_fill_index("doc_index/empty");
        t.set_query_pipeline(query_pipeline!(
            WhitespaceTokenizer
                > NumberFilter
                | Must [Any in number]
                > LowercaseFilter
                > Stemmer(Algorithm::English)
                > Must [Any in title]));
        // No term to funnel -> no operator -> only numberfunnel returns
        should_yield(&t, "10", &[0]);
        // Unkown term to funnel -> empty iterator -> empty result
        should_yield(&t, "10 pizza", &[]);
        should_yield(&t, "deimos", &[2]);
    }

    #[test]
    fn nested_query() {
        let mut t = create_and_fill_index("doc_index/nested");
        t.set_query_pipeline(query_pipeline!(
            WhitespaceTokenizer
                > NumberFilter
                | Must [Any in number]
                > LowercaseFilter
                > Stemmer(Algorithm::English)
                > Must [Any in title]
                > Must [All in text]));
        should_yield(&t, "2567 deimos phobos", &[2]);
        should_yield(&t, "deimos phobos", &[2]);
        should_yield(&t, "ocean", &[]);
    }

    #[test]
    fn filtered_query() {
        let t = create_and_fill_index("doc_index/filtered_query");
        let unfiltered = Query::new("flew".to_string());
        let filtered = Query::new("flew".to_string()).filter(t.documents.number.query_atom(&2567));

        assert_eq!(t.run_query(unfiltered).collect::<Vec<_>>(),
                   vec![Posting(DocId(0)), Posting(DocId(1)), Posting(DocId(2))]);
        assert_eq!(t.run_query(filtered).collect::<Vec<_>>(),
                   vec![Posting(DocId(2))]);
    }

    #[test]
    fn iterate_filters() {
        let mut t = create_and_fill_index("doc_index/iterate_filters");
        t.add_document(&[(Cow::from("text"), Cow::from("125 10"))]);
        t.add_document(&[(Cow::from("text"), Cow::from("10"))]);
        t.add_document(&[(Cow::from("text"), Cow::from("10"))]);
        t.commit();
        assert_eq!(t.frequent_terms_number().map(|(df, t, _)| (df, *t)).collect::<Vec<_>>(),
                   vec![(5, 10), (3, 125), (1, 2567)]);
    }
}
