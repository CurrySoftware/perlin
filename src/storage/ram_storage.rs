use std::sync::Arc;

use storage::persistence::Volatile;
use storage::{StorageError, Result, Storage};

/// Stores anything in a `Vector`.
/// Assumes the id is the index
pub struct RamStorage<T> {
    data: Vec<Arc<T>>,
}

impl<T> Volatile for RamStorage<T> {
    fn new() -> Self {
        RamStorage { data: Vec::with_capacity(8192) }
    }
}

impl<T: Sync + Send> Storage<T> for RamStorage<T> {

    fn len(&self) -> usize {
        self.data.len()
    }
    
    fn get(&self, id: u64) -> Result<Arc<T>> {
        self.data.get(id as usize).cloned().ok_or(StorageError::KeyNotFound)
    }

    
    fn store(&mut self, id: u64, data: T) -> Result<()> {
        assert_eq!(id as usize, self.data.len());
        self.data.push(Arc::new(data));
        Ok(())
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    use storage::persistence::Volatile;
    use storage::Storage;

    #[test]
    pub fn basic() {
        let posting1 = vec![(0, vec![0, 1, 2, 3, 4]), (1, vec![5])];
        let posting2 = vec![(0, vec![0, 1, 4]), (1, vec![5]), (5, vec![0, 24, 56])];
        let mut prov = RamStorage::new();
        assert!(prov.store(0, posting1.clone()).is_ok());
        assert_eq!(prov.get(0).unwrap().as_ref(), &posting1);
        assert!(prov.store(1, posting2.clone()).is_ok());
        assert_eq!(prov.get(1).unwrap().as_ref(), &posting2);
        assert!(prov.get(0).unwrap().as_ref() != &posting2);
    }

    #[test]
    pub fn not_found() {
        let prov: RamStorage<usize> = RamStorage::new();
        assert!(prov.get(0).is_err());
    }

}
