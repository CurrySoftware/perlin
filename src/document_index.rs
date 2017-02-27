use std::marker::PhantomData;

use query::Operand;
use perlin_core::index::posting::DocId;

pub type Pipeline<Out, T> = Box<Fn(DocId, &mut T, &str) -> PhantomData<Out> + Sync + Send>;
pub type QueryPipeline<T> = Box<for<'r, 'x> Fn(&'r T, &'x str) -> Operand<'r> + Sync + Send>;

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
        #[NoPipe]
        number: Field<u64>,
        #[NoPipe]
        emails: Field<usize>,
    }

    use language::{Stemmer, LowercaseFilter, WhitespaceTokenizer};
    use language::integers::NumberFilter;
    use std::borrow::Cow;
    use perlin_core::index::posting::Posting;

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

    fn should_yield(index: &TestIndex, query: &str, ids: &[u64]) {
        if index.run_query(query).collect::<Vec<_>>() !=
           ids.iter().map(|id| Posting(DocId(*id))).collect::<Vec<_>>() {
            assert!(false,
                    format!("{} resulted in {:?} expexted {:?}",
                            query,
                            index.run_query(query).collect::<Vec<_>>(),
                            ids.iter().map(|id| Posting(DocId(*id))).collect::<Vec<_>>()))
        }
    }

    #[test]
    #[should_panic]
    fn negative_test() {
        let t = create_and_fill_index("doc_index/basic_test");
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
        let mut t = create_and_fill_index("doc_index/empty");
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
}
