use syn;
use quote;

pub fn generate_index_struct(ast: &syn::MacroInput) -> quote::Tokens {
    let ident = &ast.ident;
    let index_ident = syn::Ident::from(format!("{}Index", ident).to_string());

    let ext_id = external_id_field(ast);
    let create_external_ids = create_external_ids(ast);
    let run_query = run_query(ast);

    quote!(
        pub struct #index_ident {
            pub documents: #ident,
            pub query_pipeline: Option<QueryPipeline<#ident>>,
            pub doc_counter: DocId,
            #ext_id
        }


        impl #index_ident {
            pub fn create(base_path: PathBuf) -> Self {
                #index_ident {
                    documents: #ident::create(&base_path),
                    query_pipeline: None,
                    doc_counter: DocId::none(),
                    #create_external_ids
                }
            }

            pub fn commit(&mut self) {
                self.documents.commit();
            }

            pub fn set_query_pipeline(&mut self, pipe: QueryPipeline<#ident>) {
                self.query_pipeline = Some(pipe);
            }

            #run_query
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
                        let ops = query_pipe(&self.documents, &query);
                    QueryResultIterator::new(ops, query.filter, &self.external_ids)
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


fn create_external_ids(ast: &syn::MacroInput) -> quote::Tokens {
    if let Some(_) = get_external_id_type(&ast.attrs) {
        quote!{
            external_ids: Vec::new()
        }
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
