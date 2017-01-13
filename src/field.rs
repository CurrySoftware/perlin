use std::str::FromStr;
use std::collections::HashMap;

pub struct FieldQuery(pub Field);

impl FieldQuery {
    pub fn new_string(field_id: FieldId, query: String) -> Self {
        FieldQuery(Field(field_id, FieldContent::String(query)))
    }

    pub fn new_number(field_id: FieldId, query: u64) -> Self {
        FieldQuery(Field(field_id, FieldContent::Number(query)))
    }
}

#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Copy, Clone)]
pub struct FieldId(pub u64);

pub enum FieldType {
    String,
    Number,
}

#[derive(Debug, Eq, PartialEq)]
pub enum FieldContent {
    String(String),
    Number(u64),
}

/// A field represented by its Id and its Content
#[derive(Debug, PartialEq, Eq)]
pub struct Field(pub FieldId, pub FieldContent);

impl Field {
    pub fn get_content(self) -> FieldContent {
        self.1
    }

    pub fn id(&self) -> &FieldId {
        &self.0
    }
}


pub trait FieldResolver {
    fn register_field(&mut self, String, FieldType) -> FieldId;

    fn resolve_field(&self, &str, &str) -> Result<Field, ()>;
}

impl FieldResolver for HashMap<String, (FieldId, FieldType)> {
    fn register_field(&mut self, field_name: String, field_type: FieldType) -> FieldId {
        let field_id = FieldId(self.len() as u64);
        self.insert(field_name, (field_id, field_type));
        field_id
    }
    
    fn resolve_field(&self, field_name: &str, field_content: &str) -> Result<Field, ()> {
        if let Some(&(field_id, ref field_type)) = self.get(field_name) {
            match *field_type {
                FieldType::String => {
                    return Ok(Field(field_id, FieldContent::String(field_content.to_string())));
                }
                FieldType::Number => {
                    return Ok(Field(field_id,
                                    FieldContent::Number(u64::from_str(field_content).unwrap())))
                }
            }
        }
        Err(())
    }
}
