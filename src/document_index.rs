use std::marker::PhantomData;

use query::Operand;
use perlin_core::index::posting::DocId;

pub type Pipeline<Out, T> = Box<Fn(DocId, &mut T, &str) -> PhantomData<Out> + Sync + Send>;
pub type QueryPipeline<T> = Box<for<'r, 'x> Fn(&'r T, &'x str) -> Operand<'r>>;

#[cfg(test)]
mod tests {
    use perlin_core::index::posting::DocId;
    use field::Field;

    use rust_stemmers::Algorithm;

    use test_utils::create_test_dir;

    #[derive(PerlinDocument)]
    #[ExternalId(usize)]
    pub struct Test {
        text: Field<String>,
        title: Field<String>,
        number: Field<u64>,
        emails: Field<usize>,
    }    

    use language::{Stemmer, LowercaseFilter, WhitespaceTokenizer};
    use language::integers::NumberFilter;

    #[test]
    fn basic_test() {
        use std::borrow::Cow;
        use query::{Funnel, Operator, ToOperand};
        use perlin_core::index::posting::Posting;
        let mut t = TestIndex::create(create_test_dir("doc_index/basic_test"));
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
        t.set_query_pipeline(Box::new(|docs, query| {
            use language::CanApply;
            use query::{OrConstructor};
            let mut to_op = WhitespaceTokenizer::create(
                NumberFilter::create(OrConstructor::create(Funnel::create(Operator::All, &docs.number)),
                                        LowercaseFilter::create(
                                            Stemmer::create(Algorithm::English,
                                                            Funnel::create(Operator::All, &docs.text)))));
            to_op.apply(query);
            to_op.to_operand()
        }));

        t.add_document(&[(Cow::from("text"), Cow::from("10 birds flew over MT EVEREST"))],
                       10);
        t.add_document(&[(Cow::from("text"), Cow::from("125 birds flew accross THE ocean"))],
                       10);
        t.add_document(&[(Cow::from("text"),
                          Cow::from("2567 unicorns flew from phobos to deimos"))],
                       10);
        t.commit();
        assert_eq!(t.run_query("10 deimos").collect::<Vec<_>>(), vec![Posting(DocId(0)), Posting(DocId(2))]);
        assert_eq!(t.run_query("birds deimos").collect::<Vec<_>>(), vec![]);
        assert_eq!(t.run_query("birds").collect::<Vec<_>>(), vec![Posting(DocId(0)), Posting(DocId(1))]);
        t.set_query_pipeline(
            query_pipeline!( WhitespaceTokenizer
                             > NumberFilter
                             | Must [Any in number]
                             > LowercaseFilter
                             > Stemmer(Algorithm::English)
                             > Must [All in text]
            ));
        // (AND (any in number) (Or [All in text] [Any in title]) [Any in text]
        assert_eq!(t.run_query("10 deimos").collect::<Vec<_>>(), vec![]);
        assert_eq!(t.run_query("2567 deimos").collect::<Vec<_>>(), vec![Posting(DocId(2))]);
    }
}
