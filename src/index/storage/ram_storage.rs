use std::collections::BTreeMap;
use std::sync::Arc;

use index::storage::{StorageError, Result, Storage};

/// Stores anything in a `BTreeMap`
pub struct RamStorage<T> {
    data: BTreeMap<u64, Arc<T>>,
}

impl<T> RamStorage<T> {
    pub fn new() -> Self {
        RamStorage { data: BTreeMap::new() }
    }
}

impl<T: Sync + Send> Storage<T> for RamStorage<T> {

    fn get(&self, id: u64) -> Result<Arc<T>> {
        self.data.get(&id).cloned().ok_or(StorageError::KeyNotFound)
    }

    fn store(&mut self, id: u64, data: T) -> Result<()>{
        self.data.insert(id, Arc::new(data));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use index::storage::Storage;
    
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
