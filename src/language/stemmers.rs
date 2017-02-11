use rust_stemmers::{Algorithm, Stemmer as RStemmer};

use std::borrow::Cow;
use std::marker::PhantomData;

use language::CanApply;

pub struct Stemmer<T, TCallback>
{
    stemmer: RStemmer,
    callback: TCallback,
    _ty: PhantomData<T>
}

impl<T, TCallback> CanApply<String, T> for Stemmer<T, TCallback>
    where TCallback: for<'r> CanApply<Cow<'r, str>, T>
{
    fn apply(&self, input: String, t: &mut T) {
        self.callback.apply(self.stemmer.stem(&input), t);
    }
}


impl<T, TCallback> Stemmer<T, TCallback> {
    pub fn create(language: Algorithm, callback: TCallback) -> Self {
        Stemmer{
            stemmer: RStemmer::create(language),
            callback: callback,
            _ty: PhantomData
        }
    }
}
