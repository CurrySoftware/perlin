use syn;
use quote;


// struct IdentPipes  {
//    field1: Pipeline<Ty, Ident>,
//    field2  ...
// }


pub fn generate_pipeline_struct(ast: &syn::MacroInput) -> quote::Tokens {
    let ident = &ast.ident;
    let pipes_ident = syn::Ident::from(format!("{}Pipes", ident).to_string());

    let declarations = generate_declarations(ast);
    let variant_data = if let syn::Body::Struct(ref variant_data) = ast.body {
        variant_data
    } else {
        panic!("derive(PerlinDocument) only implemented for Structs!");
    };
    let fields = variant_data.fields().iter().map(|f| f.ident.clone());
    quote!(
        struct #pipes_ident {
            #(#declarations)*
        }

        impl Default for #pipes_ident {
            fn default() -> Self {
                #pipes_ident {
                    #(#fields: None,)*
                }
            }
        }
    )
}


fn generate_declarations(ast: &syn::MacroInput) -> Vec<quote::Tokens> {
    let ident = &ast.ident;
    let mut result = Vec::new();
    let variant_data = if let syn::Body::Struct(ref variant_data) = ast.body {
        variant_data
    } else {
        panic!("derive(PerlinDocument) only implemented for Structs!");
    };

    for field in variant_data.fields() {
        let field_ident = &field.ident;
        let ty = get_generics_from_field(&field.ty);
        result.push(quote!{
            #field_ident: Option<Pipeline<#ty, #ident>>,
        });
    }
    result
}

fn get_generics_from_field(field: &syn::Ty) -> quote::Tokens {
    if let &syn::Ty::Path(_, ref path) = field {
        for segment in &path.segments {
            if segment.ident == "Field" {
                if let syn::PathParameters::AngleBracketed(ref params) = segment.parameters {
                    let params = &params.types.first();
                    return quote!(#params);
                }
            }
        }
    }
    panic!("NO FIELD FOUND!");
}
