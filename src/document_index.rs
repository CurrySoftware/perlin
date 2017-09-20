use std::marker::PhantomData;

use query::{Query, Operand, WeightingOperator};
use perlin_core::utils::seeking_iterator::PeekableSeekable;
use perlin_core::index::posting::{Posting, DocId};

pub type Pipeline<Out, T> = Box<Fn(DocId, &mut T, &str) -> PhantomData<Out> + Sync + Send>;
pub type QueryPipeline<T> =
    Box<for<'r> Fn(&'r T, &Query<'r>) -> Vec<PeekableSeekable<Operand<'r>>> + Sync + Send>;

pub struct QueryResultIterator<'a, T: 'a>(WeightingOperator<'a>, &'a [(DocId, T)]);

impl<'a, T: 'a + Clone> QueryResultIterator<'a, T> {
    pub fn new(ops: Vec<PeekableSeekable<Operand<'a>>>,
               filters: Vec<PeekableSeekable<Operand<'a>>>,
               ext_ids: &'a [(DocId, T)])
               -> Self {
        println!("New QueryResultIterator! OPS: {:?} filters: {:?}",
                 ops,
                 filters);
        QueryResultIterator(WeightingOperator::create(ops, filters), ext_ids)
    }
}

impl<'a, T: 'a + Clone> Iterator for QueryResultIterator<'a, T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(Posting(doc_id)) = self.0.next() {
            if let Ok(index) = self.1.binary_search_by_key(&doc_id, |&(d_id, _)| d_id) {
                Some(self.1[index].1.clone())
            } else {
                panic!("DocId unkown!");
            }
        } else {
            None
        }
    }
}

impl<'a, T: 'a> AsRef<WeightingOperator<'a>> for QueryResultIterator<'a, T> {
    fn as_ref(&self) -> &WeightingOperator<'a> {
        &self.0
    }
}

impl<'a, T: 'a> AsMut<WeightingOperator<'a>> for QueryResultIterator<'a, T> {
    fn as_mut(&mut self) -> &mut WeightingOperator<'a> {
        &mut self.0
    }
}


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
    use query::{Query, ChainingOperator};


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
                | [Any in number]
                > LowercaseFilter
                > Stemmer(Algorithm::English)
                > [All in text]
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
        if index.run_query(Query::new(query)).collect::<Vec<_>>() !=
           ids.iter().map(|id| Posting(DocId(*id))).collect::<Vec<_>>() {
            assert!(false,
                    format!("{} resulted in {:?} expexted {:?}",
                            query,
                            index.run_query(Query::new(query)).collect::<Vec<_>>(),
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
                | [Any in number]
                > LowercaseFilter
                > Stemmer(Algorithm::English)
                > [Any in title]));
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
                | [Any in number]
                > LowercaseFilter
                > Stemmer(Algorithm::English)
                > [All in text]));
        should_yield(&t, "2567 deimos phobos", &[2]);
        should_yield(&t, "deimos phobos", &[2]);
        should_yield(&t, "ocean", &[]);
    }

    #[test]
    fn filtered_query() {
        let t = create_and_fill_index("doc_index/filtered_query");
        let unfiltered = Query::new("flew");
        let filtered =
            Query::new("flew").filter_by(ChainingOperator::Must,
                                         t.documents.number.query_atom(&2567));

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
