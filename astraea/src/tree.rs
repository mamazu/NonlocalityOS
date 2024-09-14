use dogbox_blob_layer::BlobDigest;
use futures::future::join;
use serde::{Deserialize, Serialize};
use sha3::{Digest, Sha3_512};
use std::{
    collections::BTreeMap,
    io::{self, Write},
    pin::Pin,
    sync::{Arc, Mutex},
};

#[derive(Clone, PartialEq, PartialOrd, Ord, Eq, Hash, Debug, Copy)]
pub struct TypeId(pub u64);

#[derive(Clone, PartialEq, PartialOrd, Ord, Eq, Hash, Debug, Copy)]
pub struct Reference {
    digest: BlobDigest,
}

impl Reference {
    pub fn add_type(&self, type_id: TypeId) -> TypedReference {
        TypedReference::new(type_id, *self)
    }
}

#[derive(Clone, PartialEq, PartialOrd, Ord, Eq, Hash, Debug, Copy)]
pub struct TypedReference {
    type_id: TypeId,
    reference: Reference,
}

impl TypedReference {
    fn new(type_id: TypeId, reference: Reference) -> TypedReference {
        TypedReference {
            type_id: type_id,
            reference: reference,
        }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct Value {
    serialized: Vec<u8>,
    references: Vec<TypedReference>,
}

impl Value {
    pub fn new(serialized: Vec<u8>, references: Vec<TypedReference>) -> Value {
        Value {
            serialized: serialized,
            references: references,
        }
    }

    pub fn from_string(value: &str) -> Value {
        Value {
            serialized: value.as_bytes().to_vec(),
            references: Vec::new(),
        }
    }

    pub fn from_unit() -> Value {
        Value {
            serialized: Vec::new(),
            references: Vec::new(),
        }
    }

    pub fn to_string(&self) -> Option<String> {
        match std::str::from_utf8(&self.serialized) {
            Ok(success) => Some(success.to_string()),
            Err(_) => None,
        }
    }
}

#[derive(Clone, PartialEq, Debug)]
pub struct TypedValue {
    pub type_id: TypeId,
    pub value: Value,
}

impl TypedValue {
    pub fn new(type_id: TypeId, value: Value) -> TypedValue {
        TypedValue {
            type_id: type_id,
            value: value,
        }
    }
}

pub trait ReduceExpression: Sync + Send {
    fn reduce<'t>(
        &'t self,
        argument: TypedValue,
        service_resolver: &'t dyn ResolveServiceId,
        loader: &'t dyn LoadValue,
        storage: &'t dyn StoreValue,
    ) -> Pin<Box<dyn std::future::Future<Output = TypedValue> + 't>>;
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
    argument: TypedValue,
    service_resolver: &dyn ResolveServiceId,
    loader: &dyn LoadValue,
    storage: &dyn StoreValue,
) -> std::result::Result<TypedValue, ReductionError> {
    let service = match service_resolver.resolve(&argument.type_id) {
        Some(service) => service,
        None => return Err(ReductionError::NoServiceForType(argument.type_id)),
    };
    let result = service
        .reduce(argument, service_resolver, loader, storage)
        .await;
    Ok(result)
}

pub struct ReferencedValue {
    reference: TypedReference,
    value: Arc<Value>,
}

impl ReferencedValue {
    fn new(reference: TypedReference, value: Arc<Value>) -> ReferencedValue {
        ReferencedValue { reference, value }
    }
}

pub async fn reduce_expression_from_reference(
    argument: &TypedReference,
    service_resolver: &dyn ResolveServiceId,
    loader: &dyn LoadValue,
    storage: &dyn StoreValue,
) -> std::result::Result<ReferencedValue, ReductionError> {
    let argument_value = match loader.load_value(&argument.reference) {
        Some(loaded) => loaded,
        None => return Err(ReductionError::UnknownReference(argument.reference)),
    };
    let value = reduce_expression_without_storing_the_final_result(
        TypedValue::new(
            argument.type_id,
            /*TODO: avoid this clone*/ (*argument_value).clone(),
        ),
        service_resolver,
        loader,
        storage,
    )
    .await?;
    let arc_value = Arc::new(value.value);
    Ok(ReferencedValue::new(
        storage
            .store_value(arc_value.clone())
            .add_type(value.type_id),
        arc_value,
    ))
}

pub struct ServiceRegistry {
    services: BTreeMap<TypeId, Arc<dyn ReduceExpression>>,
}

impl ServiceRegistry {
    pub fn new(services: BTreeMap<TypeId, Arc<dyn ReduceExpression>>) -> ServiceRegistry {
        ServiceRegistry { services }
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
        argument: TypedValue,
        service_resolver: &'t dyn ResolveServiceId,
        loader: &'t dyn LoadValue,
        storage: &'t dyn StoreValue,
    ) -> Pin<Box<dyn std::future::Future<Output = TypedValue> + 't>> {
        Box::pin(async move {
            assert_eq!(2, argument.value.references.len());
            let past_ref = reduce_expression_from_reference(
                &argument.value.references[0],
                service_resolver,
                loader,
                storage,
            );
            let message_ref = reduce_expression_from_reference(
                &argument.value.references[1],
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
        argument: TypedValue,
        _service_resolver: &dyn ResolveServiceId,
        _loader: &dyn LoadValue,
        _storage: &dyn StoreValue,
    ) -> Pin<Box<dyn std::future::Future<Output = TypedValue>>> {
        Box::pin(std::future::ready(argument))
    }
}

pub fn calculate_reference(referenced: &Value) -> Reference {
    let mut hasher = Sha3_512::new();
    hasher.update(&referenced.serialized);
    for item in &referenced.references {
        hasher.update(item.type_id.0.to_be_bytes());
        hasher.update(item.reference.digest.0 .0);
        hasher.update(item.reference.digest.0 .1);
    }
    let result = hasher.finalize();
    let slice: &[u8] = result.as_slice();
    let mut chunks: std::slice::ArrayChunks<u8, 64> = slice.array_chunks();
    let chunk = chunks.next().unwrap();
    assert!(chunks.remainder().is_empty());
    Reference {
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
        InMemoryValueStorage { reference_to_value }
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
        TypedValue::new(TypeId(0), Value::from_string("hello, world!\n")),
        &services,
        &value_storage,
        &value_storage,
    )
    .await
    .unwrap();
    assert_eq!(TypeId(0), result.type_id);
    assert_eq!(
        Some("hello, world!\n".to_string()),
        result.value.to_string()
    );
}

pub fn make_text_in_console(past: TypedReference, text: TypedReference) -> TypedValue {
    TypedValue::new(
        TypeId(2),
        Value {
            serialized: Vec::new(),
            references: vec![past, text],
        },
    )
}

pub fn make_beginning_of_time() -> Value {
    Value {
        serialized: Vec::new(),
        references: vec![],
    }
}

pub fn make_effect(cause: TypedReference) -> TypedValue {
    TypedValue::new(
        TypeId(3),
        Value {
            serialized: Vec::new(),
            references: vec![cause],
        },
    )
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
    let past = value_storage
        .store_value(Arc::new(make_beginning_of_time()))
        .add_type(TypeId(3));
    let message = value_storage
        .store_value(Arc::new(Value::from_string("hello, world!\n")))
        .add_type(TypeId(0));
    let text_in_console = make_text_in_console(past, message);
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

pub fn make_seconds(amount: u64) -> TypedValue {
    TypedValue::new(
        TypeId(5),
        Value {
            serialized: amount.to_be_bytes().to_vec(),
            references: Vec::new(),
        },
    )
}

pub fn to_seconds(value: &Value) -> Option<u64> {
    let mut buf: [u8; 8] = [0; 8];
    if buf.len() != value.serialized.len() {
        return None;
    }
    buf.copy_from_slice(&value.serialized);
    Some(u64::from_be_bytes(buf))
}

pub fn make_sum(summands: Vec<TypedReference>) -> TypedValue {
    TypedValue::new(
        TypeId(6),
        Value {
            serialized: Vec::new(),
            references: summands,
        },
    )
}

pub fn to_sum(value: Value) -> Option<Vec<TypedReference>> {
    Some(value.references)
}

pub struct SumService {}

impl ReduceExpression for SumService {
    fn reduce<'t>(
        &'t self,
        argument: TypedValue,
        service_resolver: &'t dyn ResolveServiceId,
        loader: &'t dyn LoadValue,
        storage: &'t dyn StoreValue,
    ) -> Pin<Box<dyn std::future::Future<Output = TypedValue> + 't>> {
        let summands_expressions = to_sum(argument.value).unwrap();
        Box::pin(async move {
            let summands_futures: Vec<_> = summands_expressions
                .iter()
                .map(|summand| {
                    reduce_expression_from_reference(summand, service_resolver, loader, storage)
                })
                .collect();
            let summands_values = futures::future::join_all(summands_futures).await;
            let sum = summands_values.iter().fold(0u64, |accumulator, element| {
                let summand = to_seconds(&element.as_ref().unwrap().value).unwrap();
                u64::checked_add(accumulator, summand).unwrap()
            });
            make_seconds(sum)
        })
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_sum() {
    let sum_service: Arc<dyn ReduceExpression> = Arc::new(SumService {});
    let identity: Arc<dyn ReduceExpression> = Arc::new(Identity {});
    let services = ServiceRegistry {
        services: BTreeMap::from([(TypeId(5), identity), (TypeId(6), sum_service)]),
    };
    let value_storage = InMemoryValueStorage {
        reference_to_value: Mutex::new(BTreeMap::new()),
    };
    let a = value_storage
        .store_value(Arc::new(make_seconds(1).value))
        .add_type(TypeId(5));
    let b = value_storage
        .store_value(Arc::new(make_seconds(2).value))
        .add_type(TypeId(5));
    let sum = value_storage
        .store_value(Arc::new(make_sum(vec![a, b]).value))
        .add_type(TypeId(6));
    let result = reduce_expression_from_reference(&sum, &services, &value_storage, &value_storage)
        .await
        .unwrap();
    assert_eq!(make_seconds(3).value, *result.value);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_nested_sum() {
    let sum_service: Arc<dyn ReduceExpression> = Arc::new(SumService {});
    let identity: Arc<dyn ReduceExpression> = Arc::new(Identity {});
    let services = ServiceRegistry {
        services: BTreeMap::from([(TypeId(5), identity), (TypeId(6), sum_service)]),
    };
    let value_storage = InMemoryValueStorage {
        reference_to_value: Mutex::new(BTreeMap::new()),
    };
    let a = value_storage
        .store_value(Arc::new(make_seconds(1).value))
        .add_type(TypeId(5));
    let b = value_storage
        .store_value(Arc::new(make_seconds(2).value))
        .add_type(TypeId(5));
    let c = value_storage
        .store_value(Arc::new(make_sum(vec![a.clone(), b]).value))
        .add_type(TypeId(6));
    let sum = make_sum(vec![a.clone(), a, c]);
    let result = reduce_expression_without_storing_the_final_result(
        sum,
        &services,
        &value_storage,
        &value_storage,
    )
    .await
    .unwrap();
    assert_eq!(make_seconds(5), result);
}

pub fn make_delay(before: TypedReference, duration: TypedReference) -> TypedValue {
    TypedValue::new(
        TypeId(4),
        Value {
            serialized: Vec::new(),
            references: vec![before, duration],
        },
    )
}

pub struct DelayService {}

impl ReduceExpression for DelayService {
    fn reduce<'t>(
        &'t self,
        mut argument: TypedValue,
        service_resolver: &'t dyn ResolveServiceId,
        loader: &'t dyn LoadValue,
        storage: &'t dyn StoreValue,
    ) -> Pin<Box<dyn std::future::Future<Output = TypedValue> + 't>> {
        let mut arguments = argument.value.references.drain(0..2);
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
    let past = value_storage
        .store_value(Arc::new(make_beginning_of_time()))
        .add_type(TypeId(3));
    let duration = value_storage
        .store_value(Arc::new(make_seconds(/*can't waste time here*/ 0).value))
        .add_type(TypeId(5));
    let delay = value_storage
        .store_value(Arc::new(make_delay(past.clone(), duration).value))
        .add_type(TypeId(4));
    let message = value_storage
        .store_value(Arc::new(Value::from_string("hello, world!\n")))
        .add_type(TypeId(0));
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
        make_effect(
            value_storage
                .store_value(Arc::new(make_effect(past).value))
                .add_type(TypeId(3))
        ),
        result
    );
    assert_eq!(Some("hello, world!\n".to_string()), receiver.recv().await);
}

pub struct ActualConsole {}

impl ReduceExpression for ActualConsole {
    fn reduce<'t>(
        &'t self,
        argument: TypedValue,
        service_resolver: &'t dyn ResolveServiceId,
        loader: &'t dyn LoadValue,
        storage: &'t dyn StoreValue,
    ) -> Pin<Box<dyn std::future::Future<Output = TypedValue> + 't>> {
        Box::pin(async move {
            assert_eq!(2, argument.value.references.len());
            let past_ref = reduce_expression_from_reference(
                &argument.value.references[0],
                service_resolver,
                loader,
                storage,
            );
            let message_ref = reduce_expression_from_reference(
                &argument.value.references[1],
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

struct Lambda {
    variable: TypedReference,
    body: TypedReference,
}

impl Lambda {
    fn new(variable: TypedReference, body: TypedReference) -> Self {
        Self {
            variable: variable,
            body: body,
        }
    }
}

pub fn make_lambda(lambda: Lambda) -> TypedValue {
    TypedValue::new(
        TypeId(7),
        Value {
            serialized: Vec::new(),
            references: vec![lambda.variable, lambda.body],
        },
    )
}

pub fn to_lambda(value: Value) -> Option<Lambda> {
    if value.references.len() != 2 {
        return None;
    }
    Some(Lambda::new(value.references[0], value.references[1]))
}

struct LambdaApplication {
    function: TypedReference,
    argument: TypedReference,
}

impl LambdaApplication {
    fn new(function: TypedReference, argument: TypedReference) -> Self {
        Self {
            function: function,
            argument: argument,
        }
    }
}

pub fn make_lambda_application(function: TypedReference, argument: TypedReference) -> TypedValue {
    TypedValue::new(
        TypeId(8),
        Value {
            serialized: Vec::new(),
            references: vec![function, argument],
        },
    )
}

pub fn to_lambda_application(value: Value) -> Option<LambdaApplication> {
    if value.references.len() != 2 {
        return None;
    }
    Some(LambdaApplication::new(
        value.references[0],
        value.references[1],
    ))
}

fn replace_variable_recursively(
    body: &TypedReference,
    variable: &Reference,
    argument: &TypedReference,
    loader: &dyn LoadValue,
    storage: &dyn StoreValue,
) -> Option<TypedValue> {
    let body_loaded = loader.load_value(&body.reference).unwrap();
    let mut references = Vec::new();
    let mut has_replaced_something = false;
    for child in &body_loaded.references {
        if &child.reference == variable {
            references.push(argument.clone());
            has_replaced_something = true;
        } else {
            if let Some(replaced) =
                replace_variable_recursively(child, variable, argument, loader, storage)
            {
                let stored = storage
                    .store_value(Arc::new(replaced.value))
                    .add_type(replaced.type_id);
                references.push(stored);
                has_replaced_something = true;
            } else {
                references.push(*child);
            }
        }
    }
    if !has_replaced_something {
        return None;
    }
    Some(TypedValue::new(
        body.type_id,
        Value::new(body_loaded.serialized.clone(), references),
    ))
}

pub struct LambdaApplicationService {}

impl ReduceExpression for LambdaApplicationService {
    fn reduce<'t>(
        &'t self,
        argument: TypedValue,
        _service_resolver: &'t dyn ResolveServiceId,
        loader: &'t dyn LoadValue,
        storage: &'t dyn StoreValue,
    ) -> Pin<Box<dyn std::future::Future<Output = TypedValue> + 't>> {
        let lambda_application = to_lambda_application(argument.value).unwrap();
        let argument = &lambda_application.argument;
        let function = to_lambda(
            (*loader
                .load_value(&lambda_application.function.reference)
                .unwrap())
            .clone(),
        )
        .unwrap();
        let variable = &function.variable;
        Box::pin(std::future::ready(
            match replace_variable_recursively(
                &function.body,
                &variable.reference,
                argument,
                loader,
                storage,
            ) {
                Some(replaced) => replaced,
                None => TypedValue::new(
                    function.body.type_id,
                    (*loader.load_value(&function.body.reference).unwrap()).clone(),
                ),
            },
        ))
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_lambda() {
    let lambda_application_service: Arc<dyn ReduceExpression> =
        Arc::new(LambdaApplicationService {});
    let identity: Arc<dyn ReduceExpression> = Arc::new(Identity {});
    let sum_service: Arc<dyn ReduceExpression> = Arc::new(SumService {});
    let services = ServiceRegistry {
        services: BTreeMap::from([
            (TypeId(5), identity.clone()),
            (TypeId(6), sum_service),
            (TypeId(7), identity),
            (TypeId(8), lambda_application_service),
        ]),
    };
    let value_storage = InMemoryValueStorage {
        reference_to_value: Mutex::new(BTreeMap::new()),
    };
    let arg = value_storage
        .store_value(Arc::new(Value::from_string("arg")))
        .add_type(TypeId(0));
    let one = value_storage
        .store_value(Arc::new(make_seconds(1).value))
        .add_type(TypeId(5));
    let sum = value_storage
        .store_value(Arc::new(make_sum(vec![one, arg]).value))
        .add_type(TypeId(6));
    let plus_one = value_storage
        .store_value(Arc::new(make_lambda(Lambda::new(arg, sum)).value))
        .add_type(TypeId(7));
    let two = value_storage
        .store_value(Arc::new(make_seconds(2).value))
        .add_type(TypeId(5));
    let call = value_storage
        .store_value(Arc::new(make_lambda_application(plus_one, two).value))
        .add_type(TypeId(8));
    // When we apply a function to an argument we receive the body with the variable replaced.
    let reduced_once =
        reduce_expression_from_reference(&call, &services, &value_storage, &value_storage)
            .await
            .unwrap();
    // A second reduction then constant-folds the body away:
    let reduced_twice = reduce_expression_from_reference(
        &reduced_once.reference,
        &services,
        &value_storage,
        &value_storage,
    )
    .await
    .unwrap();
    assert_ne!(reduced_once.reference, reduced_twice.reference);
    assert_eq!(make_seconds(3).value, *reduced_twice.value);
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub struct CompilerError {
    pub message: String,
    pub line: u64,
    pub column: u64,
}

impl CompilerError {
    fn new(message: String, line: u64, column: u64) -> Self {
        Self {
            message,
            line,
            column,
        }
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Clone)]
pub struct CompilerOutput {
    pub entry_point: TypedReference,
    pub errors: Vec<CompilerError>,
}

impl CompilerOutput {
    pub fn new(entry_point: TypedReference, errors: Vec<CompilerError>) -> CompilerOutput {
        CompilerOutput {
            entry_point: entry_point,
            errors: errors,
        }
    }

    pub fn from_value(input: Value) -> Option<CompilerOutput> {
        if input.references.len() != 1 {
            return None;
        }
        let errors: Vec<CompilerError> = match postcard::from_bytes(&input.serialized[..]) {
            Ok(parsed) => parsed,
            Err(_) => return None,
        };
        Some(CompilerOutput::new(input.references[0], errors))
    }

    pub fn to_value(self) -> Value {
        let serialized = postcard::to_allocvec(&self.errors).unwrap();
        Value::new(serialized, vec![self.entry_point])
    }
}

struct Compiled {
    pub source: TypedReference,
}

impl Compiled {
    pub fn new(source: TypedReference) -> Compiled {
        Compiled { source: source }
    }

    pub fn from_value(input: Value) -> Option<Compiled> {
        if input.references.len() != 1 {
            return None;
        }
        Some(Compiled::new(input.references[0]))
    }

    pub fn to_value(self) -> Value {
        Value::new(Vec::new(), vec![self.source])
    }
}

pub struct CompiledReducer {}

impl ReduceExpression for CompiledReducer {
    fn reduce<'t>(
        &'t self,
        argument: TypedValue,
        service_resolver: &'t dyn ResolveServiceId,
        loader: &'t dyn LoadValue,
        storage: &'t dyn StoreValue,
    ) -> Pin<Box<dyn std::future::Future<Output = TypedValue> + 't>> {
        let source_ref = argument.value.references[0];
        let source_value = loader.load_value(&source_ref.reference).unwrap();
        let source_string = source_value.to_string().unwrap();
        let compiler_output: CompilerOutput =
            crate::compiler::compile(&source_string, loader, storage);
        Box::pin(std::future::ready(TypedValue::new(
            TypeId(10),
            compiler_output.to_value(),
        )))
    }
}