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

    // #[derive(PerlinDocument)]
    // #[ExternalId(usize)]
    pub struct Test {
        text: Field<String>,
        title: Field<String>,
        number: Field<u64>,
        emails: Field<usize>,
    }
    pub use self::perlin_impl::TestIndex;
    mod perlin_impl {
        use super::Test;
        use std::path::{Path, PathBuf};
        use std::borrow::Cow;
        use document_index::{QueryPipeline, Pipeline};
        use query::Operand;
        use perlin_core::index::posting::DocId;
        struct TestPipes {
            text: Option<Pipeline<String, Test>>,
            title: Option<Pipeline<String, Test>>,
            number: Option<Pipeline<u64, Test>>,
            emails: Option<Pipeline<usize, Test>>,
        }
        
        impl Default for TestPipes {
            fn default() -> Self {
                TestPipes {
                    text: None,
                    title: None,
                    number: None,
                    emails: None,
                }
            }
        }
        pub struct TestIndex {
            documents: Test,
            pipelines: TestPipes,
            query_pipeline: Option<QueryPipeline<Test>>,
            base_path: PathBuf,
            doc_counter: DocId,
            external_ids: Vec<(DocId, usize)>,
        }
        impl TestIndex {
            pub fn create(base_path: PathBuf) -> Self {
                TestIndex {
                    documents: Test::create(&base_path),
                    pipelines: TestPipes::default(),
                    query_pipeline: None,
                    base_path: base_path,
                    doc_counter: DocId::none(),
                    external_ids: Vec::new(),
                }
            }
            pub fn commit(&mut self) {
                self.documents.commit();
            }
            pub fn add_document(&mut self,
                                key_values: &[(Cow<str>, Cow<str>)],
                                external_id: usize) {
                self.doc_counter.inc();
                let doc_id = self.doc_counter;
                for &(ref key, ref value) in key_values {
                    match key.as_ref() {
                        "text" => {
                            if let Some(ref pipeline) = self.pipelines.text {
                                pipeline(doc_id, &mut self.documents, value.as_ref());
                            } else {
                            }
                        }
                        "title" => {
                            if let Some(ref pipeline) = self.pipelines.title {
                                pipeline(doc_id, &mut self.documents, value.as_ref());
                            } else {
                            }
                        }
                        "number" => {
                            if let Some(ref pipeline) = self.pipelines.number {
                                pipeline(doc_id, &mut self.documents, value.as_ref());
                            } else {
                            }
                        }
                        "emails" => {
                            if let Some(ref pipeline) = self.pipelines.emails {
                                pipeline(doc_id, &mut self.documents, value.as_ref());
                            } else {
                            }
                        }
                        _ => {}
                    }
                }
                self.external_ids.push((doc_id, external_id));
            }
            pub fn set_text_pipeline(&mut self, pipe: Pipeline<String, Test>) {
                self.pipelines.text = Some(pipe);
            }
            pub fn set_title_pipeline(&mut self, pipe: Pipeline<String, Test>) {
                self.pipelines.title = Some(pipe);
            }
            pub fn set_query_pipeline(&mut self, pipe: QueryPipeline<Test>) {
                self.query_pipeline = Some(pipe);
            }
            pub fn run_query<'a>(&'a self, query: &str) -> Operand<'a> {
                if let Some(ref query_pipe) = self.query_pipeline {
                    query_pipe(&self.documents, query)
                } else {
                    panic!();
                }
            }
        }
        impl Test {
            pub fn create(path: &Path) -> Self {
                use perlin_core::page_manager::{RamPageCache, FsPageManager};
                use perlin_core::index::vocabulary::SharedVocabulary;
                use perlin_core::index::Index;
                let text_page_cache =
                    RamPageCache::new(FsPageManager::new(&path.join("text_page_cache")));
                let title_page_cache =
                    RamPageCache::new(FsPageManager::new(&path.join("title_page_cache")));
                let number_page_cache =
                    RamPageCache::new(FsPageManager::new(&path.join("number_page_cache")));
                let emails_page_cache =
                    RamPageCache::new(FsPageManager::new(&path.join("emails_page_cache")));
                Test {
                    text: Index::new(text_page_cache, SharedVocabulary::new()),
                    title: Index::new(title_page_cache, SharedVocabulary::new()),
                    number: Index::new(number_page_cache, SharedVocabulary::new()),
                    emails: Index::new(emails_page_cache, SharedVocabulary::new()),
                }
            }
            pub fn commit(&mut self) {
                self.text.commit();
                self.title.commit();
                self.number.commit();
                self.emails.commit();
            }
        }
    }

    use language::{Stemmer, LowercaseFilter, WhitespaceTokenizer};
    use language::integers::NumberFilter;

    #[test]
    fn test() {
        use std::borrow::Cow;
        use query::{Funnel, Operator, ToOperand};
        use perlin_core::index::posting::Posting;
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
        t.set_query_pipeline(Box::new(|docs, query| {
            use language::CanApply;
            use query::{OrConstructor};
            let mut to_op = WhitespaceTokenizer::create(
                NumberFilter::create(OrConstructor::create(Funnel::create(Operator::And, &docs.number)),
                                        LowercaseFilter::create(
                                            Stemmer::create(Algorithm::English,
                                                            Funnel::create(Operator::And, &docs.text)))));
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
        assert_eq!(t.run_query("deimos").collect::<Vec<_>>(), vec![Posting(DocId(2))]);
    }
}
