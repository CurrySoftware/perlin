use std::vec::IntoIter;
use std::borrow::Cow;

use field::{RawField, FieldId, FieldResolver};


/// A document is represented by an abitrary number of fields
#[derive(Debug, Eq, PartialEq)]
pub struct Document<'a>(pub Vec<RawField<'a>>);

/// This builder can be used to ergonomically (really?) build documents
pub struct DocumentBuilder<'a> {
    fields: Vec<RawField<'a>>,
}

impl<'a> DocumentBuilder<'a> {
    pub fn new() -> Self {
        DocumentBuilder { fields: Vec::new() }
    }

    /// Add a new field to the document
    pub fn add_field(mut self, field: RawField<'a>) -> Self {
        self.fields.push(field);
        self
    }

    pub fn build(self) -> Document<'a> {
        Document(self.fields)
    }
}


/// Implemented by an entity that has the ability to parse documents
pub trait DocumentParser {
    fn parse_document<'a>(&self,
                          key_values: &'a [(Cow<'a, str>, Cow<'a, str>)])
                          -> Result<Document<'a>, ()>;
}
