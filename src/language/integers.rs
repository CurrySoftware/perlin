use std::marker::PhantomData;
use std::str::FromStr;

use language::CanApply;

/// Numberfilter.
/// Takes an string as input and tries to convert it to usize
/// If this is possible it calls the number_callback with the resulting usize
/// Otherwise it calls the string_callback with the original input
pub struct NumberFilter<T, TStringCallback, TNumberCallback>
{
    string_callback: TStringCallback,
    number_callback: TNumberCallback,
    _ty: PhantomData<T>
}

impl<T, TSCB, TNCB>  NumberFilter<T, TSCB, TNCB> {
    pub fn create(number_callback: TNCB,
              string_callback: TSCB) -> Self {
        NumberFilter{
            string_callback: string_callback,
            number_callback: number_callback,
            _ty: PhantomData
        }
    }
}

impl<'a, T, TStringCallback, TNumberCallback> CanApply<&'a str, T>
    for NumberFilter<T, TStringCallback, TNumberCallback>
    where TStringCallback: CanApply<&'a str, T>,
          TNumberCallback: CanApply<u64, T>
{
    type Output = TStringCallback::Output;
    fn apply(&self, input: &'a str, t: &mut T) {
        if let Ok(number) = u64::from_str(input) {
            self.number_callback.apply(number, t);
        } else {
            self.string_callback.apply(input, t);
        }
    }
}


pub struct ToU64<T, TCallback>
{
    callback: TCallback,
    _ty: PhantomData<T>
}

impl<T, TCallback> ToU64<T, TCallback> {
    pub fn create(callback: TCallback) -> Self{
        ToU64 {
            callback: callback,
            _ty: PhantomData
        }
    }
}

impl<'a, T, TCallback> CanApply<&'a str, T> for ToU64<T, TCallback>
    where TCallback: CanApply<u64, T>{
    type Output = TCallback::Output;

    fn apply(&self, input: &'a str, t: &mut T) {
        if let Ok(number) = u64::from_str(input) {
            self.callback.apply(number, t);
        }
    }
}
