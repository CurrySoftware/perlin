use std::str::FromStr;
use std::marker::PhantomData;

use document_index::TermIndexer;

use field::FieldId;
use perlin_core::index::posting::DocId;

mod stemmers;

pub use language::stemmers::Stemmer;


pub trait CanApply<Input, T> {
    fn apply(&self, Input, &mut T);
}

pub struct WhitespaceTokenizer<T, TCallback>    
{
    callback: TCallback,
    _ty: PhantomData<T>
}

impl<T, TCallback> WhitespaceTokenizer<T, TCallback> {
    pub fn create(callback: TCallback) -> Self {
        WhitespaceTokenizer {
            callback: callback,
            _ty: PhantomData
        }
    }
}

impl<'a, T, TCallback> CanApply<&'a str, T> for WhitespaceTokenizer<T, TCallback>
    where TCallback: for<'r> CanApply<&'r str, T> {
    fn apply(&self, input: &'a str, t: &mut T) {
        for token in input.split_whitespace() {
            self.callback.apply(token, t);
        }
    }
}

pub struct LowercaseFilter<T, TCallback>
{
    callback: TCallback,
    _ty: PhantomData<T>
}

impl<T, TCallback> LowercaseFilter<T, TCallback> {
    pub fn create(callback: TCallback) -> Self {
        LowercaseFilter{
            callback: callback,
            _ty: PhantomData
        }
    }
}

impl<'a, T, TCallback> CanApply<&'a str, T> for LowercaseFilter<T, TCallback>
    where TCallback: CanApply<String, T>
{
    fn apply(&self, input: &str, t: &mut T) {
        self.callback.apply(input.to_lowercase(), t)
    }
}

pub struct NumberFilter<T, TStringCallback, TNumberCallback>
{
    string_callback: TStringCallback,
    number_callback: TNumberCallback,
    _ty: PhantomData<T>
}

impl<'a, T, TStringCallback, TNumberCallback> CanApply<&'a str, T>
    for NumberFilter<T, TStringCallback, TNumberCallback>
    where TStringCallback: for<'r> CanApply<&'r str, T>,
          TNumberCallback: CanApply<usize, T>
{
    fn apply(&self, input: &str, t: &mut T) {
        if let Ok(number) = usize::from_str(input) {
            self.number_callback.apply(number, t);
        } else {
            self.string_callback.apply(input, t);
        }
    }
}

pub struct IndexerFunnel
{
    doc_id: DocId,
    field_id: FieldId
}

impl IndexerFunnel {
    pub fn create(doc_id: DocId, field_id: FieldId) -> Self {
        IndexerFunnel {
            doc_id: doc_id,
            field_id: field_id
        }
    }
}

impl<TTerm, TContainer> CanApply<TTerm, TContainer> for IndexerFunnel
    where TContainer: TermIndexer<TTerm> {
    fn apply(&self, input: TTerm, container: &mut TContainer) {
        container.index_term(self.field_id, self.doc_id, input);
    }
}


macro_rules! inner_pipeline {
    ($element:ident($($param:expr),+) > $($x:tt)*) => {
        $element::create($($param),+ , inner_pipeline!($($x)*))        
    };
    ($element:ident > $($x:tt)*) => {
        $element::create(inner_pipeline!($($x)*))
    };
    ($doc_id:expr; $field_id:expr) => {
        IndexerFunnel::create($doc_id, $field_id)
    };
    () => {}
}

#[macro_export]
macro_rules! pipeline {
    ($($x:tt)*) => {
        Box::new(move |doc_id: DocId, field_id: FieldId| {
            Box::new(inner_pipeline!($($x)* > doc_id; field_id))
        })
    }
}

