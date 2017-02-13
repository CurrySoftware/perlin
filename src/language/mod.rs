use perlin_core::index::posting::DocId;

mod stemmers;
pub mod integers;

pub use language::stemmers::Stemmer;

/// The single central trait of the push-based splittable pipeline!
/// Any element in it can be called passing a typed and generic input and a common value
pub trait CanApply<Input> {
    type Output;
    fn apply(&mut self, Input);
}

pub struct WhitespaceTokenizer<TCallback>    
{
    callback: TCallback,
}

impl<TCallback> WhitespaceTokenizer<TCallback> {
    pub fn create(callback: TCallback) -> Self {
        WhitespaceTokenizer {
            callback: callback
        }
    }
}

impl<'a, TCallback> CanApply<&'a str> for WhitespaceTokenizer<TCallback>
    where TCallback: CanApply<&'a str> {
    type Output = TCallback::Output;
    fn apply(&mut self, input: &'a str) {
        for token in input.split_whitespace() {
            self.callback.apply(token);
        }
    }
}

pub struct LowercaseFilter<TCallback>
{
    callback: TCallback,
}

impl<TCallback> LowercaseFilter<TCallback> {
    pub fn create(callback: TCallback) -> Self {
        LowercaseFilter{
            callback: callback,
        }
    }
}

impl<'a, TCallback> CanApply<&'a str> for LowercaseFilter<TCallback>
    where TCallback: CanApply<String>
{
    type Output = TCallback::Output;
    fn apply(&mut self, input: &str) {
        self.callback.apply(input.to_lowercase())
    }
}

use perlin_core::index::Index;
use std::hash::Hash;

pub struct IndexerFunnel<'a, T: 'a + Hash + Eq>
{
    doc_id: DocId,
    index: &'a mut Index<T>
}

impl<'a, T: Hash + Eq> IndexerFunnel<'a, T> {
    pub fn create(doc_id: DocId, index: &'a mut Index<T>) -> Self {
        IndexerFunnel {
            doc_id: doc_id,
            index: index
        }
    }
}
use std::fmt::Debug;
impl<'a, TTerm: 'a + Debug + Hash + Ord + Eq> CanApply<TTerm> for IndexerFunnel<'a, TTerm>{

    type Output = TTerm;
    
    fn apply(&mut self, input: TTerm) {
        println!("INDEX: {:?}", input);
        self.index.index_term(input, self.doc_id);
    }
}


macro_rules! funnel {
    ($doc_id:expr, $index:expr) => {
        IndexerFunnel::create($doc_id, $index)
    }
}

macro_rules! inner_pipeline {
    (;$INDEX:ident; ;$doc_id:expr; ;$field:ident;
     $element:ident($($param:expr),+) | [$this_field:ident] > $($x:tt)*) =>
    // ;doc_id; ;field_id; Element(params) | [field] > Next
    {
        $element::create($($param),+ ,
                         funnel!($doc_id, &mut $INDEX.$this_field.index),
                         inner_pipeline!(;$INDEX; ;$doc_id; ;$field_id; ($x)*))        
    };
    (;$INDEX:ident; ;$doc_id:expr; ;$field:ident;
     $element:ident | [$this_field:ident] > $($x:tt)*) =>
    // ;doc_id; ;field_id; Element | [field] > Next
    {
        $element::create(
            funnel!($doc_id, &mut $INDEX.$this_field.index),
            inner_pipeline!(;$INDEX; ;$doc_id; ;$field; $($x)*))        
    };
    (;$INDEX:ident; ;$doc_id:expr; ;$field:ident; $element:ident($($param:expr),+) > $($x:tt)*) =>
    // ;doc_id; ;field_id; Element(params) > Next
    {
        $element::create($($param),+ , inner_pipeline!(;$INDEX; ;$doc_id; ;$field; $($x)*))        
    };
    (;$INDEX:ident; ;$doc_id:expr; ;$field:ident; $element:ident > $($x:tt)*) =>
    // ;doc_id; ;field_id; Element > Next
    {
        $element::create(inner_pipeline!(;$INDEX; ;$doc_id; ;$field; $($x)*))
    };
    (;$INDEX:ident; ;$doc_id:expr; ;$field:ident; $element:ident($($param:expr),+)) =>
    // ;doc_id; ;field_id; Element(params)
    {
        $element::create(
            $($param),+ ,
            inner_pipeline!(;$INDEX; ;$doc_id; ;$field;))
    };
    (;$INDEX:ident; ;$doc_id:expr; ;$field:ident; $element:ident) =>
    // ;doc_id; ;field_id; Element
    {
        $element::create(inner_pipeline!(;$INDEX; ;$doc_id; ;$field;))
    };
    
    (;$INDEX:ident; ;$doc_id:expr; ;$field:ident;) => {
        IndexerFunnel::create($doc_id, &mut $INDEX.text.index)
    };
    () => {}
}

#[macro_export]
macro_rules! pipeline {
    ($INDEX:ident : $field:ident $($x:tt)*) => {
        Box::new(|| {
        Box::new(|doc_id: DocId, index: &mut $INDEX, content: &str| {
            use language::CanApply;
            use std::marker::PhantomData;
            let mut pipe = inner_pipeline!(;index; ;doc_id; ;$field; $($x)*);
            pipe.apply(content);
            PhantomData
        })})
    }
}

