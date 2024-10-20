use crate::tree::{calculate_reference, BlobDigest, Reference, TypeId, TypedReference, Value};
use rusqlite::Transaction;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

#[derive(Clone, PartialEq, Debug)]
pub enum StoreError {
    NoSpace,
    Rusqlite(String),
}

pub trait StoreValue {
    fn store_value(&self, value: Arc<Value>) -> std::result::Result<Reference, StoreError>;
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
    fn store_value(&self, value: Arc<Value>) -> std::result::Result<Reference, StoreError> {
        let mut lock = self.reference_to_value.lock().unwrap();
        let reference = calculate_reference(&value);
        if !lock.contains_key(&reference) {
            lock.insert(reference.clone(), value);
        }
        Ok(reference)
    }
}

impl LoadValue for InMemoryValueStorage {
    fn load_value(&self, reference: &Reference) -> Option<Arc<Value>> {
        let lock = self.reference_to_value.lock().unwrap();
        lock.get(reference).cloned()
    }
}

impl LoadStoreValue for InMemoryValueStorage {}

pub struct SQLiteStorage {
    connection: Mutex<rusqlite::Connection>,
}

impl SQLiteStorage {
    pub fn new(connection: Mutex<rusqlite::Connection>) -> Self {
        Self { connection }
    }

    pub fn create_schema(connection: &rusqlite::Connection) -> rusqlite::Result<()> {
        connection.pragma_update(None, "foreign_keys", "on")?;
        connection
            .execute(
                "CREATE TABLE value (
                id INTEGER PRIMARY KEY NOT NULL,
                digest BLOB UNIQUE NOT NULL,
                serialized BLOB NOT NULL,
                CONSTRAINT digest_length_matches_sha3_512 CHECK (LENGTH(digest) == 64)
            ) STRICT",
                (),
            )
            .map(|size| assert_eq!(0, size))?;
        connection
            .execute(
                "CREATE TABLE reference (
                id INTEGER PRIMARY KEY NOT NULL,
                origin INTEGER NOT NULL REFERENCES value,
                target BLOB NOT NULL,
                UNIQUE (origin, target),
                CONSTRAINT digest_length_matches_sha3_512 CHECK (LENGTH(target) == 64)
            ) STRICT",
                (),
            )
            .map(|size| assert_eq!(0, size))?;
        connection
            .execute("CREATE INDEX reference_origin ON reference (origin)", ())
            .map(|size| assert_eq!(0, size))?;
        connection
            .execute("CREATE INDEX reference_target ON reference (target)", ())
            .map(|size| assert_eq!(0, size))?;
        Ok(())
    }
}

impl StoreValue for SQLiteStorage {
    fn store_value(&self, value: Arc<Value>) -> std::result::Result<Reference, StoreError> {
        let connection_locked = self.connection.lock().unwrap();
        let reference = calculate_reference(&value);
        let origin_digest: [u8; 64] = reference.digest.into();
        let transaction = Transaction::new_unchecked(&connection_locked, rusqlite::TransactionBehavior::Deferred).unwrap(/*TODO*/);
        let existing_count: i64 = connection_locked
            .query_row_and_then(
                "SELECT COUNT(*) FROM value WHERE digest = ?",
                (&origin_digest,),
                |row| -> rusqlite::Result<_, rusqlite::Error> { row.get(0) },
            )
            .map_err(|error| StoreError::Rusqlite(format!("{:?}", &error)))?;
        match existing_count {
            0 => {}
            1 => return Ok(reference),
            _ => panic!(),
        }
        connection_locked.execute(
            "INSERT INTO value (digest, serialized) VALUES (?1, ?2)",
            (&origin_digest, &value.serialized),
        ).unwrap(/*TODO*/);
        let inserted_value_rowid = connection_locked.last_insert_rowid();
        for reference in &value.references {
            let target_digest: [u8; 64] = reference.reference.digest.into();
            connection_locked.execute(
                "INSERT INTO reference (origin, target) VALUES (?1, ?2)",
                (&inserted_value_rowid, &target_digest),
            ).unwrap(/*TODO*/);
        }
        transaction.commit().unwrap(/*TODO*/);
        Ok(reference)
    }
}

impl LoadValue for SQLiteStorage {
    fn load_value(&self, reference: &Reference) -> Option<Arc<Value>> {
        let connection_locked = self.connection.lock().unwrap();
        let digest: [u8; 64] = reference.digest.into();
        let (id, serialized) = connection_locked.query_row_and_then("SELECT id, serialized FROM value WHERE digest = ?1", 
        (&digest, )       ,
         |row| -> rusqlite::Result<_> {
            let id : i64 = row.get(0).unwrap(/*TODO*/);
            let serialized = row.get(1).unwrap(/*TODO*/);
            Ok((id, serialized))
         } ).unwrap(/*TODO*/);
        let mut stmt = connection_locked.prepare("SELECT target FROM reference WHERE origin = ?").unwrap(/*TODO*/);
        let person_iter = stmt.query_map([&id], |row| {
        let target : [u8; 64] = row.get(0)?;
        Ok(TypedReference::new(TypeId(0), Reference::new(BlobDigest::new(&target))))
    }).unwrap(/*TODO*/);
        let references: Vec<crate::tree::TypedReference> = person_iter
            .map(|maybe_error| maybe_error.unwrap(/*YOLO*/))
            .collect();
        Some(Arc::new(Value::new(serialized, references)))
    }
}

impl LoadStoreValue for SQLiteStorage {}
