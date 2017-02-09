#[macro_use]
pub mod pipeline;
pub mod stemmers;

use language::pipeline::{CanChain};
pub use language::pipeline::{CanAppend, PipelineElement};

use document_index::TermIndexer;
use field::{Field, FieldDefinition};

pub struct WhitespaceTokenizer<T> {
    next: Box<PipelineElement<T>>
}

impl<TCont, TNext: PipelineElement<TCont> + 'static> CanChain<TNext> for WhitespaceTokenizer<TCont> {
    fn chain_to(next: TNext) -> Self {
        WhitespaceTokenizer{
            next: Box::new(next)
        }
    }
}

impl<T> PipelineElement<T> for WhitespaceTokenizer<T> {
    fn apply(&self, input: &str, b: &mut T) {
        for token in input.split_whitespace(){
            self.next.apply(token, b);
        }
    }
}


pub struct StringFunnel {
    field_def: FieldDefinition 
}

impl StringFunnel {
    pub fn new(field_def: FieldDefinition) -> Self {
        StringFunnel{
            field_def: field_def
        }
    }
}

impl<T> PipelineElement<T> for StringFunnel where T: TermIndexer<String> {
    fn apply(&self, input: &str, b: &mut T) {
        b.index_term(Field(self.field_def, input.to_owned()));
    }
}
