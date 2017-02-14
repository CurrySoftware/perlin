use syn;
use quote;

pub fn generate_perlin_document_impl(ast: &syn::MacroInput) -> quote::Tokens {
    let ident = &ast.ident;
    let variant_data = if let syn::Body::Struct(ref variant_data) = ast.body {
        variant_data
    } else {
        panic!("derive(PerlinDocument) only implemented for Structs!");
    };
    let page_caches = generate_page_caches(variant_data.fields());
    let index_creations = generate_index_creations(variant_data.fields());
    let fields = variant_data.fields().iter().map(|f| f.ident.clone());
    
    quote!(        
        impl #ident {            
            pub fn create(path: &Path) -> Self {
                use perlin_core::page_manager::{RamPageCache, FsPageManager};
                use perlin_core::index::vocabulary::SharedVocabulary;
                use perlin_core::index::Index;
                #(#page_caches)*
                
                #ident {
                    #(#index_creations,)*
                }
            }

            pub fn commit(&mut self) {
                #(self.#fields.commit();)*                
            }
        }
    )
}

fn generate_page_caches(fields: &[syn::Field]) -> Vec<quote::Tokens> {
    let mut result = Vec::new();

    for field in fields {
        let cache_ident = syn::Ident::from(format!("{}_page_cache", &field.ident.clone().unwrap()).to_string());
        result.push(quote!(
            let #cache_ident = RamPageCache::new(FsPageManager::new(&path.join(stringify!(#cache_ident))));));
    }
    
    result
}

fn generate_index_creations(fields: &[syn::Field]) -> Vec<quote::Tokens> {
    let mut result = Vec::new();
    for field in fields {
        let cache_ident = syn::Ident::from(format!("{}_page_cache", &field.ident.clone().unwrap()).to_string());
        let ident = &field.ident;
        result.push(quote!(
            #ident: Index::new(#cache_ident, SharedVocabulary::new())
        ));
    }
    result
}
