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

#[proc_macro_derive(PerlinDocument, attributes(ExternalId, NoPipe))]
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
            use super::#ident;

            use std::path::{Path, PathBuf};
            use std::borrow::Cow;
            
            use perlin::document_index::Pipeline;
            use perlin_core::index::posting::DocId;
            
            #pipeline_struct

            #index_struct

            #perlin_doc_impl
        }
    }
}


// fn impl_perlin_document(ast: &syn::MacroInput) -> quote::Tokens {
//     let name = &ast.ident;
//     let index_name = syn::Ident::from(format!("{}Index", name).to_string());

//     if let syn::Body::Struct(ref variant_data) = ast.body {
//         let commit = commit(variant_data.fields());
//         let index_field = index_field(variant_data.fields());

//         let params = create_params(variant_data.fields());
//         let page_caches = create_page_caches(variant_data.fields());
//         let field_creations = create_field_creations(variant_data.fields());
        
//         quote! {
//             // pub struct #index_name {
//             //     documents: #name,
                
//             // }

            
//             pub mod perlin_document_impl {
//                 use super::*;
                
//                 use std::path::Path;
                
//                 impl #name {
//                     pub fn create(path: &Path #(,#params)*) -> Self {
//                         use perlin_core::page_manager::{RamPageCache, FsPageManager};
//                         #(#page_caches)*
                        
//                         #name {
//                             #(#field_creations,)*
//                         }
//                     }
//                 }
                
//                 impl PerlinDocument for #name {
//                     fn commit(&mut self) {
//                         #(#commit)*
//                     }
                    
//                     fn index_field(&mut self, doc_id: DocId, field_name: &str, field_contents: &str) {
//                         match field_name {                       
//                             #(#index_field,)*
//                             _ => {panic!("WHAT!?")}
//                         };
//                     }
//                 }            
//             }
//         }
//     } else {
//         panic!("PerlinDocument is only implemented for structs not enums!");
//     }    
// }

fn create_params(fields: &[syn::Field]) -> Vec<quote::Tokens> {
    let mut result = Vec::new();

    for field in fields {
        let ident = &field.ident;
        let gen_params = get_generics_from_field(&field.ty);

        result.push(quote!(#ident: Option<Pipeline#gen_params>));
    }
    result
}

fn get_generics_from_field(field: &syn::Ty) -> quote::Tokens {
    if let &syn::Ty::Path(_, ref path) = field {
        for segment in &path.segments {
            if segment.ident == "Field" {
                let params = &segment.parameters;;
                return quote!(#params);
            }
        }
    }
    panic!("NO FIELD FOUND!");
}



fn commit(fields: &[syn::Field]) -> Vec<quote::Tokens> {
    let mut result = Vec::new();

    for field in fields {
        let ident = &field.ident;
        result.push(quote! {
            self.#ident.index.commit();
        });
    }
    result
}

fn index_field(fields: &[syn::Field]) -> Vec<quote::Tokens> {
    let mut result = Vec::new();

    for field in fields {
        let ident = &field.ident;
        result.push(quote! {
            stringify!(#ident) =>
            {
                let pipe = if let Some(ref pipeline) = self.#ident.pipeline
                {
                    pipeline()
                }
                else {
                    panic!(concat!("No pipeline found for ", stringify!(#ident)))
                };
                pipe(doc_id, self, field_contents);                
            }});
    }
    result
}
