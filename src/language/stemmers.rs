use rust_stemmers::{Algorithm, Stemmer as RStemmer};

use query::{Operand, ToOperand};
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
    fn apply(&mut self, input: String) {
        self.callback.apply(self.stemmer.stem(&input).into_owned());
    }
}

impl<'a, TCallback> CanApply<&'a str> for Stemmer<TCallback>
    where TCallback: CanApply<String>
{
    type Output = TCallback::Output;
    fn apply(&mut self, input: &'a str) {
        self.callback.apply(self.stemmer.stem(input).into_owned());
    }
}

impl<'a, TCallback> ToOperand<'a> for Stemmer<TCallback>
    where TCallback: ToOperand<'a> {
    fn to_operand(self) -> Operand<'a> {
        self.callback.to_operand()
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
