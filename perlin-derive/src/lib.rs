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
    let ast = syn::parse_macro_input(&s).expect("AST: WHAT!?");

    let gen = impl_perlin_document(&ast);
    println!("GEN:::\n\n {:#?}", gen);
    gen.parse().expect("GEN: WHAT!?")
}


fn impl_perlin_document(ast: &syn::MacroInput) -> quote::Tokens {
    let name = &ast.ident;
    if let syn::Body::Struct(ref variant_data) = ast.body {
        let commit = commit(variant_data.fields());
        let index_field = index_field(variant_data.fields());

        let params = create_params(variant_data.fields());
        let page_caches = create_page_caches(variant_data.fields());
        let field_creations = create_field_creations(variant_data.fields());
        
        quote! {
            use std::path::Path;
            impl #name {
                pub fn create(path: &Path #(,#params)*) -> Self {
                    use perlin_core::page_manager::{RamPageCache, FsPageManager};
                    #(#page_caches)*

                    #name {
                        #(#field_creations,)*
                    }
                }
            }
            
            impl PerlinDocument for #name {
                fn commit(&mut self) {
                    #(#commit)*
                }
                
                fn index_field(&mut self, doc_id: DocId, field_name: &str, field_contents: &str) {
                    let pipeline = match field_name {                       
                        #(#index_field,)*
                        _ => {panic!("WHAT!?")}
                    };
                    pipeline.apply(field_contents, self);
                }
            }            
        }
    } else {
        panic!("PerlinDocument is only implemented for structs not enums!");
    }    
}

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

fn create_page_caches(fields: &[syn::Field]) -> Vec<quote::Tokens> {
    let mut result = Vec::new();

    for field in fields {
        let cache_ident = syn::Ident::from(format!("{}_page_cache", &field.ident.clone().unwrap()).to_string());
        result.push(quote!(
            let #cache_ident = RamPageCache::new(FsPageManager::new(&path.join(stringify!(#cache_ident))));));
    }
    
    result
}

fn create_field_creations(fields: &[syn::Field]) -> Vec<quote::Tokens> {
    let mut result = Vec::new();
    for field in fields {
        let cache_ident = syn::Ident::from(format!("{}_page_cache", &field.ident.clone().unwrap()).to_string());
        let ident = &field.ident;
        result.push(quote!(
            #ident: Field::create(#cache_ident, #ident)
            ));
    }

    result
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
            stringify!(#ident) => if let Some(ref pipeline) = self.#ident.pipeline { pipeline(doc_id) } else { panic!("No pipeline found for ") }
            });
    }
    result
}
