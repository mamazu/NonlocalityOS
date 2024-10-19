use crate::tree::{calculate_reference, Reference, Value};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

pub trait StoreValue {
    fn store_value(&self, value: Arc<Value>) -> Reference;
}

pub trait LoadValue {
    fn load_value(&self, reference: &Reference) -> Option<Arc<Value>>;
}

pub trait LoadStoreValue: LoadValue + StoreValue {}

impl std::fmt::Debug for dyn LoadStoreValue + Send + Sync {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "LoadStoreValue")
    }
}

pub struct InMemoryValueStorage {
    reference_to_value: Mutex<BTreeMap<Reference, Arc<Value>>>,
}

impl InMemoryValueStorage {
    pub fn new(reference_to_value: Mutex<BTreeMap<Reference, Arc<Value>>>) -> InMemoryValueStorage {
        InMemoryValueStorage { reference_to_value }
    }

    pub fn empty() -> InMemoryValueStorage {
        Self {
            reference_to_value: Mutex::new(BTreeMap::new()),
        }
    }

    pub fn len(&self) -> usize {
        self.reference_to_value.lock().unwrap().len()
    }
}

impl StoreValue for InMemoryValueStorage {
    fn store_value(&self, value: Arc<Value>) -> Reference {
        let mut lock = self.reference_to_value.lock().unwrap();
        let reference = calculate_reference(&value);
        if !lock.contains_key(&reference) {
            lock.insert(reference.clone(), value);
        }
        reference
    }
}

impl LoadValue for InMemoryValueStorage {
    fn load_value(&self, reference: &Reference) -> Option<Arc<Value>> {
        let lock = self.reference_to_value.lock().unwrap();
        lock.get(reference).cloned()
    }
}

impl LoadStoreValue for InMemoryValueStorage {}
