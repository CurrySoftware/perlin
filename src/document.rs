use std::vec::IntoIter;

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
    pub fn add_field(mut self,
                     field_name: &str,
                     field_content: &'a str,
                     resolver: &FieldResolver)
                     -> Result<Self, ()> {
        self.fields.push(resolver.resolve(field_name, field_content)?);
        Ok(self)
    }

    pub fn build(self) -> Document<'a> {
        Document(self.fields)
    }
}
