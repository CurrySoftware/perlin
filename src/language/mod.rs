use std::marker::PhantomData;

use perlin_core::index::posting::DocId;

mod stemmers;
pub mod integers;

pub use language::stemmers::Stemmer;

/// The single central trait of the push-based splittable pipeline!
/// Any element in it can be called passing a typed and generic input and a common value
pub trait CanApply<Input, T> {
    type Output;
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
    where TCallback: CanApply<&'a str, T> {
    type Output = TCallback::Output;
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
    type Output = TCallback::Output;
    fn apply(&self, input: &str, t: &mut T) {
        self.callback.apply(input.to_lowercase(), t)
    }
}


// pub struct IndexerFunnel<'a, T>
// {
//     doc_id: DocId,
// }

// impl IndexerFunnel {
//     pub fn create(doc_id: DocId) -> Self {
//         IndexerFunnel {
//             doc_id: doc_id
//         }
//     }
// }

// impl<TTerm, TContainer> CanApply<TTerm, TContainer> for IndexerFunnel
//     where TContainer: TermIndexer<TTerm> {

//     type Output = TTerm;
    
//     fn apply(&self, input: TTerm, container: &mut TContainer) {
//         container.index_term(self.field_id, self.doc_id, input);
//     }
// }


macro_rules! funnel {
    ($doc_id:expr, $field_id:expr) => {
        //IndexerFunnel::create($doc_id, $field_id)
    }
}

macro_rules! inner_pipeline {
    (;$doc_id:expr; ;$field_id:expr;
     $element:ident($($param:expr),+) | [$this_field_id:expr] > $($x:tt)*) =>
    // ;doc_id; ;field_id; Element(params) | [field] > Next
    {
        $element::create($($param),+ ,
                         funnel!($doc_id, $this_field_id),
                         inner_pipeline!(;$doc_id; ;$field_id; ($x)*))        
    };
    (;$doc_id:expr; ;$field_id:expr;
     $element:ident | [$this_field_id:expr] > $($x:tt)*) =>
    // ;doc_id; ;field_id; Element | [field] > Next
    {
        $element::create(
            funnel!($doc_id, $this_field_id),
            inner_pipeline!(;$doc_id; ;$field_id; $($x)*))        
    };
    (;$doc_id:expr; ;$field_id:expr; $element:ident($($param:expr),+) > $($x:tt)*) =>
    // ;doc_id; ;field_id; Element(params) > Next
    {
        $element::create($($param),+ , inner_pipeline!(;$doc_id; ;$field_id; $($x)*))        
    };
    (;$doc_id:expr; ;$field_id:expr; $element:ident > $($x:tt)*) =>
    // ;doc_id; ;field_id; Element > Next
    {
        $element::create(inner_pipeline!(;$doc_id; ;$field_id; $($x)*))
    };
    (;$doc_id:expr; ;$field_id:expr; $element:ident($($param:expr),+)) =>
    // ;doc_id; ;field_id; Element(params)
    {
        $element::create(
            $($param),+ ,
            inner_pipeline!(;$doc_id; ;$field_id;))
    };
    (;$doc_id:expr; ;$field_id:expr; $element:ident) =>
    // ;doc_id; ;field_id; Element
    {
        $element::create(inner_pipeline!(;$doc_id; ;$field_id;))
    };
    
    (;$doc_id:expr; ;$field_id:expr;) => {
       // IndexerFunnel::create($doc_id, $field_id)
    };
    () => {}
}

#[macro_export]
macro_rules! pipeline {
    ($($x:tt)*) => {
        Box::new(move |doc_id: DocId, field_id: FieldId| {
            Box::new(inner_pipeline!(;doc_id; ;field_id; $($x)*))
        })
    }
}

