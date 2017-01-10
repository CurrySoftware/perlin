use std::vec::IntoIter;

use field::{Field, FieldId, FieldContent};


/// A document is represented by an abitrary number of fields
pub struct Document(pub Vec<Field>);

impl Document {
    
    /// Consumes the document and returns an iterator over its fields by value
    pub fn take_fields(self) -> IntoIter<Field> {
        self.0.into_iter()
    }
}

/// This builder can be used to ergonomically (really?) build documents
pub struct DocumentBuilder{
    fields: Vec<Field>,
}

impl DocumentBuilder {
    
    pub fn new() -> Self {
        DocumentBuilder {
            fields: Vec::new()
        }
    }
    // Add a new field to this document
    pub fn add_string_field(mut self, field_id: FieldId, content: String) -> Self {
        self.fields.push(Field(field_id, FieldContent::String(content)));
        self
    }

    pub fn build(self) -> Document {
        Document(self.fields)
    }
}
