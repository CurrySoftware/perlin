use std::marker::PhantomData;

use perlin_core::index::posting::DocId;

pub type Pipeline<Out, T> = Box<Fn(DocId, &mut T, &str) -> PhantomData<Out>>;

#[cfg(test)]
mod tests {
    use document_index::Pipeline;
    use perlin_core::index::posting::DocId;
    use field::Field;

    use rust_stemmers::Algorithm;

    use std::path::{Path, PathBuf};

    use test_utils::create_test_dir;

    #[derive(PerlinDocument)]
    #[ExternalId(usize)]
    struct Test {
        text: Field<String>,
        title: Field<String>,
        number: Field<u64>,
        emails: Field<usize>,
    }
    // struct TestPipes {
    //     text: Option<Pipeline<String, Test>>,
    //     title: Option<Pipeline<String, Test>>,
    //     number: Option<Pipeline<u64, Test>>,
    //     emails: Option<Pipeline<usize, Test>>,
    // }
    // impl Default for TestPipes {
    //     fn default() -> Self {
    //         TestPipes {
    //             text: None,
    //             title: None,
    //             number: None,
    //             emails: None,
    //         }
    //     }
    // }
    // pub struct TestIndex {
    //     documents: Test,
    //     pipelines: TestPipes,
    //     doc_counter: DocId,
    //     external_ids: Vec<(DocId, usize)>,
    // }
    // impl TestIndex {
    //     pub fn create(base_path: PathBuf) -> Self {
    //         TestIndex {
    //             documents: Test::create(&base_path),
    //             pipelines: TestPipes::default(),
    //             doc_counter: DocId::none(),
    //             external_ids: Vec::new(),
    //         }
    //     }
    //     pub fn commit(&mut self) {
    //         self.documents.commit();
    //     }
    //     pub fn add_document(&mut self, key_values: &[(&str, &str)], external_id: usize) {
    //         self.doc_counter.inc();
    //         let doc_id = self.doc_counter;
    //         for &(key, value) in key_values {
    //             match key {
    //                 "text" => {
    //                     if let Some(ref pipeline) = self.pipelines.text {
    //                         pipeline(doc_id, &mut self.documents, value);
    //                     }
    //                 }
    //                 "title" => {
    //                     if let Some(ref pipeline) = self.pipelines.title {
    //                         pipeline(doc_id, &mut self.documents, value);
    //                     }
    //                 }
    //                 "number" => {
    //                     if let Some(ref pipeline) = self.pipelines.number {
    //                         pipeline(doc_id, &mut self.documents, value);
    //                     }
    //                 }
    //                 "emails" => {
    //                     if let Some(ref pipeline) = self.pipelines.emails {
    //                         pipeline(doc_id, &mut self.documents, value);
    //                     }
    //                 },
    //                 _ => {}
    //             }
    //         }
    //         self.external_ids.push((doc_id, external_id));
    //     }
    //     fn set_text_pipeline(&mut self, pipe: Pipeline<String, Test>) {
    //         self.pipelines.text = Some(pipe);
    //     }
    //     fn set_title_pipeline(&mut self, pipe: Pipeline<String, Test>) {
    //         self.pipelines.title = Some(pipe);
    //     }
    //     fn set_number_pipeline(&mut self, pipe: Pipeline<u64, Test>) {
    //         self.pipelines.number = Some(pipe);
    //     }
    //     fn set_emails_pipeline(&mut self, pipe: Pipeline<usize, Test>) {
    //         self.pipelines.emails = Some(pipe);
    //     }
    // }

    // impl Test {
    //     pub fn create(path: &Path) -> Self {
    //         use perlin_core::page_manager::{RamPageCache, FsPageManager};
    //         use perlin_core::index::vocabulary::SharedVocabulary;
    //         use perlin_core::index::Index;
    //         let text_page_cache =
    //             RamPageCache::new(FsPageManager::new(&path.join("text_page_cache")));
    //         let title_page_cache =
    //             RamPageCache::new(FsPageManager::new(&path.join("title_page_cache")));
    //         let number_page_cache =
    //             RamPageCache::new(FsPageManager::new(&path.join("number_page_cache")));
    //         let emails_page_cache =
    //             RamPageCache::new(FsPageManager::new(&path.join("emails_page_cache")));
    //         Test {
    //             text: Index::new(text_page_cache, SharedVocabulary::new()),
    //             title: Index::new(title_page_cache, SharedVocabulary::new()),
    //             number: Index::new(number_page_cache, SharedVocabulary::new()),
    //             emails: Index::new(emails_page_cache, SharedVocabulary::new()),
    //         }
    //     }

    //     pub fn commit(&mut self) {
    //         self.text.commit();
    //         self.title.commit();
    //         self.number.commit();
    //         self.emails.commit();
    //     }
    // }

    use language::{Stemmer, LowercaseFilter, IndexerFunnel, WhitespaceTokenizer};
    use language::integers::NumberFilter;

    #[test]
    fn test() {
        use perlin_core::index::posting::Posting;
        let mut t = TestIndex::create(create_test_dir("doc_index/test"));
        t.set_text_pipeline(pipeline!(Test: text
                           WhitespaceTokenizer
                           > NumberFilter
                           | [number]
                           > LowercaseFilter
                      > Stemmer(Algorithm::English)));
        t.set_title_pipeline(pipeline!(Test: title
                      WhitespaceTokenizer
                      > LowercaseFilter
                      > Stemmer(Algorithm::English)));

        t.add_document(&[("text", "10 birds flew over MT EVEREST")], 10);
        t.add_document(&[("text", "125 birds flew accross THE ocean")], 10);
        t.add_document(&[("text", "2567 unicorns flew from phobos to deimos")], 10);        
        t.commit();
        assert_eq!(t.documents.text.query_atom(&"bird".to_string()),
                   vec![Posting(DocId(0)), Posting(DocId(1))]);
        assert_eq!(t.documents.text.query_atom(&"unicorn".to_string()),
                   vec![Posting(DocId(2))]);
        assert_eq!(t.documents.number.query_atom(&125), vec![Posting(DocId(1))]);
    }
}
