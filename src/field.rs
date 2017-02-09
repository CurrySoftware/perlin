use language::pipeline::PipelineElement;

/// Uniquely identifies a field
#[derive(Copy, Clone, Debug, Ord, PartialOrd, PartialEq, Eq)]
pub struct FieldId(pub u64);

/// Possible Types of Fields
#[derive(Copy, Clone, Debug, Eq, PartialEq)]
pub enum FieldType {
    Text,
    Number,
}

/// A field definition storing its id and its type
#[derive(Debug, Eq, Copy, Clone, PartialEq)]
pub struct FieldDefinition(pub FieldId, pub FieldType);

/// A field which has yet to be processed!
#[derive(Debug, Eq, PartialEq)]
pub struct RawField<'a>(pub FieldDefinition, pub &'a str);


/// Defines functions necessary for a field resolver.
pub trait FieldResolver {
    /// Register a new field for later resolving. The name has to be unique.
    fn register_field(&mut self, name: &str, field_type: FieldType) -> Result<(), ()>;
    /// Pass in raw data to the resolver. Get a back "typed" result!
    /// Returns an `Err` if `name` was not previously registered
    fn resolve<'a>(&self, name: &str) -> Result<FieldDefinition, ()>;
}


pub struct Field<T>(pub FieldDefinition, pub T);
