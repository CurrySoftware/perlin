#[derive(Ord, PartialOrd, Eq, PartialEq, Copy, Clone)]
pub struct FieldId(pub u64);

pub enum FieldContent  {
    String(String),
    DiscreteNumber(u64),
}

/// A field represented by its Id and its Content
pub struct Field(pub FieldId, pub FieldContent);

impl Field {
    pub fn get_content(self) -> FieldContent {
        self.1
    }

    pub fn id(&self) -> &FieldId {
        &self.0
    }
}
