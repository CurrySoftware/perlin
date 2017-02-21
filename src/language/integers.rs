use std::str::FromStr;

use language::CanApply;

use query::{ChainedOperand, ToOperands};

/// Numberfilter.
/// Takes an string as input and tries to convert it to usize
/// If this is possible it calls the number_callback with the resulting usize
/// Otherwise it calls the string_callback with the original input
pub struct NumberFilter<TStringCallback, TNumberCallback> {
    string_callback: TStringCallback,
    number_callback: TNumberCallback,
}

impl<TSCB, TNCB> NumberFilter<TSCB, TNCB> {
    pub fn create(number_callback: TNCB, string_callback: TSCB) -> Self {
        NumberFilter {
            string_callback: string_callback,
            number_callback: number_callback,
        }
    }
}

impl<'a, TStringCallback, TNumberCallback> CanApply<&'a str>
    for NumberFilter<TStringCallback, TNumberCallback>
    where TStringCallback: CanApply<&'a str>,
          TNumberCallback: CanApply<u64>
{
    type Output = TStringCallback::Output;
    fn apply(&mut self, input: &'a str) {
        if let Ok(number) = u64::from_str(input) {
            self.number_callback.apply(number);
        } else {
            self.string_callback.apply(input);
        }
    }
}

impl<'a, TStringCallback, TNumberCallback> ToOperands<'a> for NumberFilter<TStringCallback, TNumberCallback>
    where TStringCallback: ToOperands<'a>,
          TNumberCallback: ToOperands<'a>
{
    fn to_operands(self) -> Vec<ChainedOperand<'a>> {
        let mut result = self.number_callback.to_operands();
        result.append(&mut self.string_callback.to_operands());
        result
    }
}


pub struct ToU64<TCallback> {
    callback: TCallback,
}

impl<TCallback> ToU64<TCallback> {
    pub fn create(callback: TCallback) -> Self {
        ToU64 { callback: callback }
    }
}

impl<'a, TCallback> CanApply<&'a str> for ToU64<TCallback>
    where TCallback: CanApply<u64>
{
    type Output = TCallback::Output;

    fn apply(&mut self, input: &'a str) {
        if let Ok(number) = u64::from_str(input) {
            self.callback.apply(number);
        }
    }
}
