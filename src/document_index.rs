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
        t.add_document(&[(Cow::from("text"), Cow::from("10 birds flew over MT EVEREST"))]);
        t.add_document(&[(Cow::from("text"), Cow::from("125 birds flew accross THE ocean"))]);
        t.add_document(&[(Cow::from("title"), Cow::from("Unicorns on Deimos")),
                         (Cow::from("text"),
                          Cow::from("2567 unicorns flew from phobos to deimos"))]);
        t.commit();
        t
    }

    #[test]
    fn basic_test() {
        let mut t = create_and_fill_index("doc_index/basic_test");
        t.set_query_pipeline(query_pipeline!(
            WhitespaceTokenizer
                > NumberFilter
                | Must [Any in number]
                > LowercaseFilter
                > Stemmer(Algorithm::English)
                > Must [All in text]
        ));
        assert_eq!(t.run_query("10 deimos").collect::<Vec<_>>(), vec![]);
        assert_eq!(t.run_query("2567 deimos").collect::<Vec<_>>(), vec![Posting(DocId(2))]);
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
        assert_eq!(t.run_query("10 deimos").collect::<Vec<_>>(), vec![]);
        assert_eq!(t.run_query("2567 deimos").collect::<Vec<_>>(), vec![Posting(DocId(2))]);
        assert_eq!(t.run_query("10").collect::<Vec<_>>(), vec![Posting(DocId(0))]);
        //Need empty PostingIterator for that to work!
        assert_eq!(t.run_query("10 pizza").collect::<Vec<_>>(), vec![]);
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
        assert_eq!(t.run_query("10 deimos").collect::<Vec<_>>(), vec![]);
        assert_eq!(t.run_query("2567 deimos phobos").collect::<Vec<_>>(), vec![Posting(DocId(2))]);
        assert_eq!(t.run_query("deimos phobos").collect::<Vec<_>>(), vec![Posting(DocId(2))]);
        assert_eq!(t.run_query("ocean").collect::<Vec<_>>(), vec![]);        
    }
}
