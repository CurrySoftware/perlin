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
    let run_query = run_query(ast);
    let field_matches = field_matches(variant_data.fields());
    let frequent_terms = frequent_terms(variant_data.fields());
    let create_sorted_fields = create_sorted_fields(variant_data.fields());
    let sorted_fields_param = sorted_fields_param(variant_data.fields());
    let construct_sorted_fields = construct_sorted_fields(variant_data.fields());
    quote!(
        pub struct #index_ident {
            pub documents: #ident,
            pipelines: #pipes_ident,
            pub query_pipeline: Option<QueryPipeline<#ident>>,
            pub doc_counter: DocId,
            #(#sorted_fields_param)*
            #ext_id
        }


        impl #index_ident {
            pub fn create(base_path: PathBuf) -> Self {
                #index_ident {
                    documents: #ident::create(&base_path),
                    pipelines: #pipes_ident::default(),
                    query_pipeline: None,
                    doc_counter: DocId::none(),
                    #(#create_sorted_fields)*
                    #create_external_ids
                }
            }

            pub fn commit(&mut self) {
                self.documents.commit();

                #(#construct_sorted_fields)*
            }

            pub fn add_document(&mut self, key_values: &[(Cow<str>, Cow<str>)] #external_id_param) {
                self.doc_counter.inc();
                let doc_id = self.doc_counter;

                for &(ref key, ref value) in key_values {
                    match key.as_ref() {
                        //"field_name" =>
                        //self.pipelines.field_name(doc_id, &mut self.documents, value);
                        #(#field_matches,)*
                        _ => {
                           // panic!("#ident not found!")
                        }
                    }
                }
                #add_external_id
            }

            pub fn set_query_pipeline(&mut self, pipe: QueryPipeline<#ident>) {
                self.query_pipeline = Some(pipe);
            }

            #run_query

            //Pipeline setter
            //fn set_field_pipeline(&mut self, pipe: Pipeline<Type, Ident>)
            #(#pipeline_setters)*

            //Frequent terms
            //fn frequent_terms_field(&self) ->
            //Box<Iterator<Item = (usize, &TTerm, PostingIterator)>>
            #(#frequent_terms)*
        }
    )
}

fn run_query(ast: &syn::MacroInput) -> quote::Tokens {
    if let Some(ext_id_type) = get_external_id_type(&ast.attrs) {
        quote!{
            pub fn run_query<'a>(&'a self, query: Query<'a>) ->
                QueryResultIterator<'a, #ext_id_type> {
                use perlin_core::index::posting::Posting;
                if let Some(ref query_pipe) = self.query_pipeline {
                    QueryResultIterator::new(query_pipe(&self.documents, query), &self.external_ids)
                } else {
                    panic!("Query Pipe not set!");
                }
            }
        }
    } else {
        quote!{
            pub fn run_query<'a>(&'a self, query: Query<'a>) -> Operand<'a> {
                if let Some(ref query_pipe) = self.query_pipeline {
                    query_pipe(&self.documents, query)
                } else {
                    panic!("Query Pipe not set!");
                }
            }
        }
    }
}

/// Generates typed setters for indexing pipelines
/// Runs over all fields of the derived struct and implements a setter for
/// each of them
/// Ignores fields with a #[no_pipe]-Attribute
fn set_pipelines(fields: &[syn::Field], ident: &syn::Ident) -> Vec<quote::Tokens> {
    let mut result = Vec::new();
    for field in fields {
        if field.attrs.iter().any(|attr| attr.name() == "no_pipe") {
            continue;
        }
        let field_ident = &field.ident;
        let fn_ident = syn::Ident::from(format!("set_{}_pipeline", field_ident.clone().unwrap())
                                            .to_string());
        let ty = get_generics_from_field(&field.ty);
        result.push(quote!{
            pub fn #fn_ident(&mut self, pipe: Pipeline<#ty, #ident>) {
                self.pipelines.#field_ident = Some(pipe);
            }
        });
    }

    result
}


/// Generates the frequent_terms method for all fields with a `filter`
/// attribute!
fn frequent_terms(fields: &[syn::Field]) -> Vec<quote::Tokens> {
    let mut result = Vec::new();
    for field in fields {
        if !field.attrs.iter().any(|attr| attr.name() == "filter") {
            continue;
        }
        let field_ident = &field.ident;
        let fn_ident = syn::Ident::from(format!("frequent_terms_{}", field_ident.clone().unwrap())
                                            .to_string());
        let sorted_ident = syn::Ident::from(format!("sorted_{}", field_ident.clone().unwrap())
                                                .to_string());
        let ty = get_generics_from_field(&field.ty);

        result.push(quote!{
            pub fn #fn_ident<'a>(&'a self) ->
                Box<Iterator<Item = (usize, &#ty, TermId)> + 'a> {
                Box::new(self.#sorted_ident.iter().map(move |&(ref df, ref t, ref term_id)| {
                    (*df, t, *term_id)
                }))
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
        result.push(quote!{
                stringify!(#ident) => {
                    if let Some(ref pipeline) = self.pipelines.#ident {
                        pipeline(doc_id, &mut self.documents, value.as_ref());
                    } else {
                        //panic!("Tried to index field #ident without initialized pipeline!")
                    }
                }
            });
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

// For struct creation
fn create_sorted_fields(fields: &[syn::Field]) -> Vec<quote::Tokens> {
    let mut result = Vec::new();
    for field in fields {
        if !field.attrs.iter().any(|attr| attr.name() == "filter") {
            continue;
        }

        let field_ident = &field.ident;
        let sorted_ident = syn::Ident::from(format!("sorted_{}", field_ident.clone().unwrap()).to_string());

        result.push(quote!{
            #sorted_ident: Vec::new(),
        })

    }
    result
}

// For use inside of the commit function
fn construct_sorted_fields(fields: &[syn::Field]) -> Vec<quote::Tokens> {
    let mut result = Vec::new();
    for field in fields {
        if !field.attrs.iter().any(|attr| attr.name() == "filter") {
            continue;
        }
        let field_ident = &field.ident;
        let sorted_ident = syn::Ident::from(format!("sorted_{}", field_ident.clone().unwrap())
                                                .to_string());
        result.push(quote!{
            let mut #sorted_ident = self.documents.#field_ident.iterate_terms().map(|(t, term_id)| {
                (self.documents.#field_ident.term_df(term_id), t.clone(), *term_id)
            }).collect::<Vec<_>>();

            #sorted_ident.sort_by(|a, b| a.0.cmp(&b.0).reverse());
            self.#sorted_ident = #sorted_ident;
        });
    }
    result
}

// For Struct definition
fn sorted_fields_param(fields: &[syn::Field]) -> Vec<quote::Tokens> {
    let mut result = Vec::new();
    for field in fields {
        if !field.attrs.iter().any(|attr| attr.name() == "filter") {
            continue;
        }
        let field_ident = &field.ident;
        let sorted_ident = syn::Ident::from(format!("sorted_{}", field_ident.clone().unwrap())
                                                .to_string());
        let ty = get_generics_from_field(&field.ty);

        result.push(quote!{
            #sorted_ident: Vec<(usize, #ty, TermId)>,
        });
    }
    result
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
        quote!(pub external_ids: Vec<(DocId, #ext_id)>,)
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
