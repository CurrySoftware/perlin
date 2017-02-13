extern crate proc_macro;
extern crate syn;
#[macro_use]
extern crate quote;

use proc_macro::TokenStream;

#[proc_macro_derive(PerlinDocument)]
pub fn perlin_document(input: TokenStream) -> TokenStream {
    input
}
