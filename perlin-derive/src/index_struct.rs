use syn;
use quote;

pub fn generate_index_struct(ast: &syn::MacroInput) -> quote::Tokens {
    let ident = &ast.ident;
    let index_ident = syn::Ident::from(format!("{}Index", ident).to_string());
    let pipes_ident = syn::Ident::from(format!("{}Pipes", ident).to_string());
    
    let variant_data = if let syn::Body::Struct(ref variant_data) = ast.body {
        variant_data
    } else {
        panic!("derive(PerlinDocument) only implemented for Structs!");
    };
    
    let ext_id = external_id_field(ast);    
    let external_id_param = external_id_param(ast);
    let add_external_id = add_external_id(ast);
    let create_external_ids = create_external_ids(ast);
    let pipeline_setters = set_pipelines(variant_data.fields(), ident);
    
    let field_matches = field_matches(variant_data.fields());
    quote!(
        pub struct #index_ident {
            documents: #ident,
            pipelines: #pipes_ident,
            base_path: PathBuf,
            doc_counter: DocId,
            #ext_id
        }


        impl #index_ident {
            pub fn create(base_path: PathBuf) -> Self {
                #index_ident {
                    documents: #ident::create(&base_path),
                    pipelines: #pipes_ident::default(),
                    base_path: base_path,
                    doc_counter: DocId::none(),
                    #create_external_ids
                }
            }
            
            pub fn commit(&mut self) {
                self.documents.commit();                
            }

            pub fn add_document(&mut self, key_values: &[(&str, &str)] #external_id_param) {
                self.doc_counter.inc();
                let doc_id = self.doc_counter;

                for &(key, value) in key_values {
                    match key {
                        //"field_name" =>  self.pipelines.field_name(doc_id, &mut self.documents, value);
                        #(#field_matches,)*
                        _ => {
                           // panic!("#ident not found!")
                        }
                    }
                }
                #add_external_id
            }

            //Pipeline setter
            //fn set_field_pipeline(&mut self, pipe: Pipeline<Type, Ident>)
            #(#pipeline_setters)*
        }        
    )
}

fn set_pipelines(fields: &[syn::Field], ident: &syn::Ident) -> Vec<quote::Tokens> {
    let mut result = Vec::new();
    for field in fields {
        let field_ident = &field.ident;
        let fn_ident = syn::Ident::from(format!("set_{}_pipeline", field_ident.clone().unwrap()).to_string());
        let ty = get_generics_from_field(&field.ty);
        result.push(quote!{
            fn #fn_ident(&mut self, pipe: Pipeline<#ty, #ident>) {
                self.pipelines.#field_ident = Some(pipe);
            }
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

fn field_matches(fields: &[syn::Field]) -> Vec<quote::Tokens> {
    let mut result = Vec::new();
    for field in fields {
        let ident = &field.ident;
        result.push(
            quote!{
                stringify!(#ident) => {
                    if let Some(ref pipeline) = self.pipelines.#ident {
                        pipeline(doc_id, &mut self.documents, value);                       
                    } else {
                      // panic!("Tried to index field #ident without initialized pipeline!")
                    }
                }
            }
        );
    }
    result
}

fn create_external_ids(ast: &syn::MacroInput) -> quote::Tokens {
    if let Some(_) = get_external_id_type(&ast.attrs) {
        quote!{
            external_ids: Vec::new()
        }
    } else {
        quote!()
    }
}
    
fn add_external_id(ast: &syn::MacroInput) -> quote::Tokens {
    if let Some(_) = get_external_id_type(&ast.attrs) {
        quote!{
            self.external_ids.push((doc_id, external_id));
        }
    } else {
        quote!()
    }
}

fn external_id_param(ast: &syn::MacroInput) -> quote::Tokens {
    if let Some(ext_id) = get_external_id_type(&ast.attrs) {
        quote!(, external_id: #ext_id)
    } else {
        quote!()
    }
}

fn external_id_field(ast: &syn::MacroInput) -> quote::Tokens {
    if let Some(ext_id) = get_external_id_type(&ast.attrs) {
        quote!(external_ids: Vec<(DocId, #ext_id)>,)
    } else {
        quote!()
    }
}

fn get_external_id_type(attributes: &[syn::Attribute]) -> Option<syn::NestedMetaItem> {
    for attribute in attributes {
        if attribute.name() == "ExternalId" {
            if let syn::MetaItem::List(_, ref nested_items) = attribute.value {                
                return Some(nested_items[0].clone());
            }
        }
    }
    None
}
