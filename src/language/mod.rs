#[macro_use]
pub mod pipeline;
pub mod stemmers;

pub use language::pipeline::{Pipeline};
use language::pipeline::PipelineElement;

pub struct WhitespaceTokenizer{}

impl PipelineElement for WhitespaceTokenizer{
    fn apply<'a>(&self, input: &str, mut callback: Box<FnMut(&str) + 'a>) {
        for token in input.split_whitespace(){
            callback(token)
        }
    }
}

pub struct LowercaseFilter{}

impl PipelineElement for LowercaseFilter{
    fn apply<'a>(&self, input: &str, mut callback: Box<FnMut(&str) + 'a>) {
         callback(&input.to_lowercase())
    }
}
