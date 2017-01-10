#[derive(Ord, PartialOrd, Eq, PartialEq, Copy, Clone)]
pub struct FieldId(pub u64);

pub enum FieldContent  {
    String(String),
    DiscreteNumber(u64),
}

pub struct Field(pub FieldId, pub FieldContent);

impl Field {
    pub fn get_content(self) -> FieldContent {
        self.1
    }

    pub fn id(&self) -> &FieldId {
        &self.0
    }
}

pub struct Document {
    pub external_id: usize,
    pub fields: Vec<Field>,
}

pub struct DocumentBuilder{
    external_id: usize,
    fields: Vec<Field>,
}

impl DocumentBuilder {
    pub fn new(external_id: usize) -> Self {
        DocumentBuilder {
            external_id: external_id,
            fields: Vec::new()
        }
    }

    pub fn add_string_field(mut self, field_id: FieldId, content: String) -> Self {
        self.fields.push(Field(field_id, FieldContent::String(content)));
        self
    }

    pub fn build(self) -> Document {
        Document {
            external_id: self.external_id,
            fields: self.fields
        }
    }
}
