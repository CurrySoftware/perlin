use std::marker::PhantomData;

use perlin_core::index::posting::DocId;

pub type Pipeline<Out, T> = Box<Fn(DocId, &mut T, &str) -> PhantomData<Out> + Sync + Send>;

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
    fn test() {
        use std::borrow::Cow;
        let mut t = TestIndex::create(create_test_dir("doc_index/test"));
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

        t.add_document(&[(Cow::from("text"), Cow::from("10 birds flew over MT EVEREST"))], 10);
        t.add_document(&[(Cow::from("text"), Cow::from("125 birds flew accross THE ocean"))], 10);
        t.add_document(&[(Cow::from("text"), Cow::from("2567 unicorns flew from phobos to deimos"))], 10);        
        t.commit();
    }
}
