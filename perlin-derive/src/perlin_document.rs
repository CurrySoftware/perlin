use syn;
use quote;

pub fn generate_perlin_document_impl(ast: &syn::MacroInput) -> quote::Tokens {
    let ident = &ast.ident;
    let variant_data = if let syn::Body::Struct(ref variant_data) = ast.body {
        variant_data
    } else {
        panic!("derive(PerlinDocument) only implemented for Structs!");
    };
    let index_creations = generate_index_creations(variant_data.fields());
    let fields = variant_data.fields().iter().map(|f| f.ident.clone());
    
    quote!(        
        impl #ident {            
            pub fn create(path: &Path) -> Self {                
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

fn generate_index_creations(fields: &[syn::Field]) -> Vec<quote::Tokens> {
    let mut result = Vec::new();
    for field in fields {
        let type_ident = get_type_ident(&field.ty).unwrap();
        let ident = &field.ident;
        result.push(quote!(
            #ident: #type_ident::new()
        ));            
    }
    result
}


fn get_type_ident(ty: &syn::Ty) -> Option<&syn::Ident> {
    if let &syn::Ty::Path(_, ref path) = ty {
        Some(&path.segments.last().unwrap().ident)
    } else {
        None
    }
}
