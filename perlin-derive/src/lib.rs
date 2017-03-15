#![recursion_limit="100"]

extern crate proc_macro;
extern crate syn;
#[macro_use]
extern crate quote;

mod pipeline_struct;
mod index_struct;
mod perlin_document;

use pipeline_struct::generate_pipeline_struct;
use index_struct::generate_index_struct;
use perlin_document::generate_perlin_document_impl;

use proc_macro::TokenStream;

#[proc_macro_derive(PerlinDocument, attributes(ExternalId, no_pipe))]
pub fn perlin_document(input: TokenStream) -> TokenStream {
    // Standard procedure when it comes to custom derive
    // See https://doc.rust-lang.org/book/procedural-macros.html
    let s = input.to_string();
    let ast = syn::parse_macro_input(&s).expect("AST: WHAT!?");

    let gen = impl_perlin_document(&ast);
    gen.parse().expect("GEN: WHAT!?")
}

fn impl_perlin_document(ast: &syn::MacroInput) -> quote::Tokens {    
    //We create three different things:
    //1. A struct that holds the pipelines
    // It looks like:
    // IdentPipes {
    //   text: Option<Pipeline<String, Ident>>
    // }
    let pipeline_struct = generate_pipeline_struct(ast);

    //2. A Wrapping struct that holds the indices as well as the pipes
    // Plus additional information
    // It looks like
    // IdentIndex {
    //    documents: Ident,
    //    pipes: IdentPipes,
    //    doc_counter: DocId,
    //    base_path: PathBuf,
    //    (external_ids: Vec<(DocId, TExternalId)>)
    //  }
    let index_struct = generate_index_struct(ast);

    //3. the impl of PerlinDocument for Ident
    let perlin_doc_impl = generate_perlin_document_impl(ast);

    let ident = &ast.ident;
    let index_ident = syn::Ident::from(format!("{}Index", ident).to_string());
    quote! {
        pub use self::perlin_impl::#index_ident;
        mod perlin_impl{            
            use super::*;

            use std::path::{Path, PathBuf};
            use std::borrow::Cow;
            
            use_parent_crate!(document_index::Pipeline);
            use_parent_crate!(document_index::QueryPipeline);
            use_parent_crate!(query::Operand);
            
            use perlin_core::index::posting::DocId;
            
            #pipeline_struct

            #index_struct

            #perlin_doc_impl
        }
    }
}
