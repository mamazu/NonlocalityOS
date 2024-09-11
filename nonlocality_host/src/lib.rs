#![feature(array_chunks)]
use dogbox_blob_layer::BlobDigest;
use futures::future::join;
use sha3::{Digest, Sha3_512};
use std::{
    collections::BTreeMap,
    io::{self, Write},
    pin::Pin,
    sync::{Arc, Mutex},
};

#[derive(Clone, PartialEq, PartialOrd, Ord, Eq, Debug)]
pub struct TypeId(pub u64);

#[derive(Clone, PartialEq, PartialOrd, Ord, Eq, Debug)]
pub struct Reference {
    type_id: TypeId,
    digest: BlobDigest,
}

#[derive(Clone, PartialEq, Debug)]
pub struct Value {
    type_id: TypeId,
    serialized: Vec<u8>,
    references: Vec<Reference>,
}

impl Value {
    pub fn from_string(value: &str) -> Value {
        Value {
            type_id: TypeId(0),
            serialized: value.as_bytes().to_vec(),
            references: Vec::new(),
        }
    }

    pub fn from_unit() -> Value {
        Value {
            type_id: TypeId(1),
            serialized: Vec::new(),
            references: Vec::new(),
        }
    }

    pub fn to_string(&self) -> Option<String> {
        if self.type_id != TypeId(0) {
            return None;
        }
        match std::str::from_utf8(&self.serialized) {
            Ok(success) => Some(success.to_string()),
            Err(_) => None,
        }
    }
}

pub trait ReduceExpression: Sync + Send {
    fn reduce<'t>(
        &'t self,
        argument: Value,
        service_resolver: &'t dyn ResolveServiceId,
        loader: &'t dyn LoadValue,
        storage: &'t dyn StoreValue,
    ) -> Pin<Box<dyn std::future::Future<Output = Value> + 't>>;
}

#[derive(Clone, PartialEq, Debug)]
pub enum ReductionError {
    NoServiceForType(TypeId),
    Io,
    UnknownReference(Reference),
}

pub trait ResolveServiceId {
    fn resolve(&self, service_id: &TypeId) -> Option<Arc<dyn ReduceExpression>>;
}

pub async fn reduce_expression_without_storing_the_final_result(
    argument: Value,
    service_resolver: &dyn ResolveServiceId,
    loader: &dyn LoadValue,
    storage: &dyn StoreValue,
) -> std::result::Result<Value, ReductionError> {
    let service = match service_resolver.resolve(&argument.type_id) {
        Some(service) => service,
        None => return Err(ReductionError::NoServiceForType(argument.type_id)),
    };
    let result = service
        .reduce(argument, service_resolver, loader, storage)
        .await;
    Ok(result)
}

pub async fn reduce_expression(
    argument: Value,
    service_resolver: &dyn ResolveServiceId,
    loader: &dyn LoadValue,
    storage: &dyn StoreValue,
) -> std::result::Result<Reference, ReductionError> {
    let value = reduce_expression_without_storing_the_final_result(
        argument,
        service_resolver,
        loader,
        storage,
    )
    .await?;
    Ok(storage.store_value(Arc::new(value)))
}

pub struct ReferencedValue {
    reference: Reference,
    value: Arc<Value>,
}

impl ReferencedValue {
    fn new(reference: Reference, value: Arc<Value>) -> ReferencedValue {
        ReferencedValue {
            reference: reference,
            value: value,
        }
    }
}

pub async fn reduce_expression_from_reference(
    argument: &Reference,
    service_resolver: &dyn ResolveServiceId,
    loader: &dyn LoadValue,
    storage: &dyn StoreValue,
) -> std::result::Result<ReferencedValue, ReductionError> {
    let argument_value = match loader.load_value(argument) {
        Some(loaded) => loaded,
        None => return Err(ReductionError::UnknownReference(argument.clone())),
    };
    let value = reduce_expression_without_storing_the_final_result(
        /*TODO: avoid this clone*/ (*argument_value).clone(),
        service_resolver,
        loader,
        storage,
    )
    .await?;
    Ok(ReferencedValue::new(
        storage.store_value(Arc::new(value)),
        argument_value,
    ))
}

pub struct ServiceRegistry {
    services: BTreeMap<TypeId, Arc<dyn ReduceExpression>>,
}

impl ServiceRegistry {
    pub fn new(services: BTreeMap<TypeId, Arc<dyn ReduceExpression>>) -> ServiceRegistry {
        ServiceRegistry { services: services }
    }
}

impl ResolveServiceId for ServiceRegistry {
    fn resolve(&self, service_id: &TypeId) -> Option<Arc<dyn ReduceExpression>> {
        self.services.get(service_id).cloned()
    }
}

pub struct TestConsole {
    writer: tokio::sync::mpsc::UnboundedSender<String>,
}

impl ReduceExpression for TestConsole {
    fn reduce<'t>(
        &'t self,
        argument: Value,
        service_resolver: &'t dyn ResolveServiceId,
        loader: &'t dyn LoadValue,
        storage: &'t dyn StoreValue,
    ) -> Pin<Box<dyn std::future::Future<Output = Value> + 't>> {
        Box::pin(async move {
            assert_eq!(2, argument.references.len());
            let past_ref = reduce_expression_from_reference(
                &argument.references[0],
                service_resolver,
                loader,
                storage,
            );
            let message_ref = reduce_expression_from_reference(
                &argument.references[1],
                service_resolver,
                loader,
                storage,
            );
            let (past_result, message_result) = join(past_ref, message_ref).await;
            let past = past_result.unwrap();
            let message_string = message_result.unwrap().value.to_string().unwrap();
            self.writer.send(message_string).unwrap();
            make_effect(past.reference)
        })
    }
}

pub struct Identity {}

impl ReduceExpression for Identity {
    fn reduce(
        &self,
        argument: Value,
        service_resolver: &dyn ResolveServiceId,
        _loader: &dyn LoadValue,
        storage: &dyn StoreValue,
    ) -> Pin<Box<dyn std::future::Future<Output = Value>>> {
        Box::pin(std::future::ready(argument))
    }
}

pub fn calculate_reference(referenced: &Value) -> Reference {
    let mut hasher = Sha3_512::new();
    hasher.update(&referenced.type_id.0.to_be_bytes());
    hasher.update(&referenced.serialized);
    for item in &referenced.references {
        hasher.update(&item.type_id.0.to_be_bytes());
        hasher.update(&item.digest.0 .0);
        hasher.update(&item.digest.0 .1);
    }
    let result = hasher.finalize();
    let slice: &[u8] = result.as_slice();
    let mut chunks: std::slice::ArrayChunks<u8, 64> = slice.array_chunks();
    let chunk = chunks.next().unwrap();
    assert!(chunks.remainder().is_empty());
    Reference {
        type_id: referenced.type_id.clone(),
        digest: BlobDigest::new(chunk),
    }
}

pub trait StoreValue {
    fn store_value(&self, value: Arc<Value>) -> Reference;
}

pub trait LoadValue {
    fn load_value(&self, reference: &Reference) -> Option<Arc<Value>>;
}

pub struct InMemoryValueStorage {
    reference_to_value: Mutex<BTreeMap<Reference, Arc<Value>>>,
}

impl InMemoryValueStorage {
    pub fn new(reference_to_value: Mutex<BTreeMap<Reference, Arc<Value>>>) -> InMemoryValueStorage {
        InMemoryValueStorage {
            reference_to_value: reference_to_value,
        }
    }
}

impl StoreValue for InMemoryValueStorage {
    fn store_value(&self, value: Arc<Value>) -> Reference {
        let mut lock = self.reference_to_value.lock().unwrap();
        let reference = calculate_reference(&*value);
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

#[tokio::test(flavor = "multi_thread")]
async fn test_reduce_expression() {
    let identity: Arc<dyn ReduceExpression> = Arc::new(Identity {});
    let services = ServiceRegistry {
        services: BTreeMap::from([(TypeId(0), identity.clone()), (TypeId(1), identity)]),
    };
    let value_storage = InMemoryValueStorage {
        reference_to_value: Mutex::new(BTreeMap::new()),
    };
    let result = reduce_expression_without_storing_the_final_result(
        Value::from_string("hello, world!\n"),
        &services,
        &value_storage,
        &value_storage,
    )
    .await
    .unwrap();
    assert_eq!(Some("hello, world!\n".to_string()), result.to_string());
}

pub fn make_text_in_console(past: Reference, text: Reference) -> Value {
    Value {
        type_id: TypeId(2),
        serialized: Vec::new(),
        references: vec![past, text],
    }
}

pub fn make_beginning_of_time() -> Value {
    Value {
        type_id: TypeId(3),
        serialized: Vec::new(),
        references: vec![],
    }
}

pub fn make_effect(cause: Reference) -> Value {
    Value {
        type_id: TypeId(3),
        serialized: Vec::new(),
        references: vec![cause],
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_effect() {
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let test_console: Arc<dyn ReduceExpression> = Arc::new(TestConsole { writer: sender });
    let identity: Arc<dyn ReduceExpression> = Arc::new(Identity {});
    let services = ServiceRegistry {
        services: BTreeMap::from([
            (TypeId(0), identity.clone()),
            (TypeId(1), identity.clone()),
            (TypeId(2), test_console),
            (TypeId(3), identity),
        ]),
    };

    let value_storage = InMemoryValueStorage {
        reference_to_value: Mutex::new(BTreeMap::new()),
    };
    let past = value_storage.store_value(Arc::new(make_beginning_of_time()));
    let message = value_storage.store_value(Arc::new(Value::from_string("hello, world!\n")));
    let text_in_console = make_text_in_console(past.clone(), message);
    let result = reduce_expression_without_storing_the_final_result(
        text_in_console,
        &services,
        &value_storage,
        &value_storage,
    )
    .await
    .unwrap();
    assert_eq!(make_effect(past), result);
    assert_eq!(Some("hello, world!\n".to_string()), receiver.recv().await);
}

pub fn make_seconds(amount: u64) -> Value {
    Value {
        type_id: TypeId(5),
        serialized: amount.to_be_bytes().to_vec(),
        references: Vec::new(),
    }
}

pub fn to_seconds(value: &Value) -> Option<u64> {
    if value.type_id != TypeId(5) {
        return None;
    }
    let mut buf: [u8; 8] = [0; 8];
    if buf.len() != value.serialized.len() {
        return None;
    }
    buf.copy_from_slice(&value.serialized);
    Some(u64::from_be_bytes(buf))
}

pub fn make_delay(before: Reference, duration: Reference) -> Value {
    Value {
        type_id: TypeId(4),
        serialized: Vec::new(),
        references: vec![before, duration],
    }
}

pub struct DelayService {}

impl ReduceExpression for DelayService {
    fn reduce<'t>(
        &'t self,
        mut argument: Value,
        service_resolver: &'t dyn ResolveServiceId,
        loader: &'t dyn LoadValue,
        storage: &'t dyn StoreValue,
    ) -> Pin<Box<dyn std::future::Future<Output = Value> + 't>> {
        let mut arguments = argument.references.drain(0..2);
        let before_ref = arguments.next().unwrap();
        let duration_ref = arguments.next().unwrap();
        assert!(arguments.next().is_none());
        Box::pin(async move {
            let before_future =
                reduce_expression_from_reference(&before_ref, service_resolver, loader, storage);
            let duration_future =
                reduce_expression_from_reference(&duration_ref, service_resolver, loader, storage);
            let (before_result, duration_result) = join(before_future, duration_future).await;
            let duration = duration_result.unwrap().value;
            let seconds = to_seconds(&duration).unwrap();
            tokio::time::sleep(std::time::Duration::from_secs(seconds)).await;
            make_effect(before_result.unwrap().reference)
        })
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_delay() {
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let test_console: Arc<dyn ReduceExpression> = Arc::new(TestConsole { writer: sender });
    let delay_service: Arc<dyn ReduceExpression> = Arc::new(DelayService {});
    let identity: Arc<dyn ReduceExpression> = Arc::new(Identity {});
    let services = ServiceRegistry {
        services: BTreeMap::from([
            (TypeId(0), identity.clone()),
            (TypeId(1), identity.clone()),
            (TypeId(2), test_console),
            (TypeId(3), identity.clone()),
            (TypeId(4), delay_service),
            (TypeId(5), identity),
        ]),
    };

    let value_storage = InMemoryValueStorage {
        reference_to_value: Mutex::new(BTreeMap::new()),
    };
    let past = value_storage.store_value(Arc::new(make_beginning_of_time()));
    let duration =
        value_storage.store_value(Arc::new(make_seconds(/*can't waste time here*/ 0)));
    let delay = value_storage.store_value(Arc::new(make_delay(past.clone(), duration)));
    let message = value_storage.store_value(Arc::new(Value::from_string("hello, world!\n")));
    let text_in_console = make_text_in_console(delay, message);
    let result = reduce_expression_without_storing_the_final_result(
        text_in_console,
        &services,
        &value_storage,
        &value_storage,
    )
    .await
    .unwrap();
    assert_eq!(
        make_effect(value_storage.store_value(Arc::new(make_effect(past)))),
        result
    );
    assert_eq!(Some("hello, world!\n".to_string()), receiver.recv().await);
}

pub struct ActualConsole {}

impl ReduceExpression for ActualConsole {
    fn reduce<'t>(
        &'t self,
        argument: Value,
        service_resolver: &'t dyn ResolveServiceId,
        loader: &'t dyn LoadValue,
        storage: &'t dyn StoreValue,
    ) -> Pin<Box<dyn std::future::Future<Output = Value> + 't>> {
        Box::pin(async move {
            assert_eq!(2, argument.references.len());
            let past_ref = reduce_expression_from_reference(
                &argument.references[0],
                service_resolver,
                loader,
                storage,
            );
            let message_ref = reduce_expression_from_reference(
                &argument.references[1],
                service_resolver,
                loader,
                storage,
            );
            let (past_result, message_result) = join(past_ref, message_ref).await;
            let past = past_result.unwrap();
            let message_string = message_result.unwrap().value.to_string().unwrap();
            print!("{}", &message_string);
            io::stdout().flush().unwrap();
            make_effect(past.reference)
        })
    }
}
