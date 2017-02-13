use perlin_core::index::posting::DocId;


/// Trait which is implemented by users of this library.
/// Please try to use the `PerlinDocument` procedural macro to implement this trait if possible!
pub trait PerlinDocument {
    /// Commits all indices
    fn commit(&mut self);

    /// Indexes a new field of a document
    fn index_field(&mut self, doc_id: DocId, field_name: &str, field_contents: &str);
}

