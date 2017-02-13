use rust_stemmers::{Algorithm, Stemmer as RStemmer};

use std::marker::PhantomData;

use language::CanApply;

pub struct Stemmer<TCallback>
{
    stemmer: RStemmer,
    callback: TCallback,
}

impl<TCallback> CanApply<String> for Stemmer<TCallback>
    where TCallback: CanApply<String>
{
    type Output = TCallback::Output;
    fn apply(&self, input: String) {
        self.callback.apply(self.stemmer.stem(&input).into_owned());
    }
}


impl<TCallback> Stemmer<TCallback> {
    pub fn create(language: Algorithm, callback: TCallback) -> Self {
        Stemmer{
            stemmer: RStemmer::create(language),
            callback: callback,
        }
    }
}
