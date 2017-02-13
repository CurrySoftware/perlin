extern crate proc_macro;
extern crate syn;
#[macro_use]
extern crate quote;

use proc_macro::TokenStream;

#[proc_macro_derive(PerlinDocument)]
pub fn perlin_document(input: TokenStream) -> TokenStream {
    // Standard procedure when it comes to custom derive
    // See https://doc.rust-lang.org/book/procedural-macros.html
    let s = input.to_string();
    let ast = syn::parse_macro_input(&s).unwrap();

    let gen = impl_perlin_document(&ast);

    gen.parse().unwrap()
}


fn impl_perlin_document(ast: &syn::MacroInput) -> quote::Tokens {
    let name = &ast.ident;
    if let syn::Body::Struct(ref variant_data) = ast.body {
        let commit = commit(variant_data.fields());
        let index_field = index_field(variant_data.fields());
        quote! {
        impl PerlinDocument for #name {
            fn commit(&mut self) {
                #(#commit)*
            }

            fn index_field(&mut self, doc_id: DocId, field_name: &str, field_contents: &str) {
                let pipeline = match(field_name) {
                    #(#index_field,)*
                    //TODO: Match pipeline for field 1 field 2 etc.
                    _ => {panic!()}
                }
                pipeline(field_contents, self);
            }
        }        
    }
    } else {
        panic!("PerlinDocument is only implemented for structs not enums!");
    }
    
    
}


fn commit(fields: &[syn::Field]) -> Vec<quote::Tokens> {
    let mut result = Vec::new();

    for field in fields {
        let ident = &field.ident;
        result.push(quote! {
            self.#ident.commit();
        });
    }
    result
}

fn index_field(fields: &[syn::Field]) -> Vec<quote::Tokens> {
    let mut result = Vec::new();

    for field in fields {
        let ident = &field.ident;
        result.push(quote! {
            "#ident" => self.#ident.pipeline(doc_id)
            });
    }
    result
}
