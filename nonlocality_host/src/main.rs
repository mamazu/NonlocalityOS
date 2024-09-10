#![feature(array_chunks)]
use dogbox_blob_layer::BlobDigest;
use futures::StreamExt;
use ratatui::{
    crossterm::{
        self,
        event::{Event, KeyCode},
    },
    widgets::Paragraph,
};
use sha3::{Digest, Sha3_512};
use std::{collections::BTreeMap, pin::Pin, sync::Arc, time::Duration};

#[derive(Clone, PartialEq, PartialOrd, Ord, Eq, Debug)]
struct TypeId(u64);

#[derive(Clone, PartialEq, PartialOrd, Ord, Eq, Debug)]
struct Reference {
    type_id: TypeId,
    digest: BlobDigest,
}

#[derive(Clone, PartialEq, Debug)]
struct Value {
    type_id: TypeId,
    serialized: Vec<u8>,
    references: Vec<Reference>,
}

impl Value {
    fn from_string(value: &str) -> Value {
        Value {
            type_id: TypeId(0),
            serialized: value.as_bytes().to_vec(),
            references: Vec::new(),
        }
    }

    fn from_unit() -> Value {
        Value {
            type_id: TypeId(1),
            serialized: Vec::new(),
            references: Vec::new(),
        }
    }

    fn to_string(&self) -> Option<String> {
        if self.type_id != TypeId(0) {
            return None;
        }
        match std::str::from_utf8(&self.serialized) {
            Ok(success) => Some(success.to_string()),
            Err(_) => None,
        }
    }
}

trait ReduceExpression: Sync + Send {
    fn reduce(
        &self,
        argument: Value,
        loader: &dyn LoadValue,
    ) -> Pin<Box<dyn std::future::Future<Output = Value>>>;
}

#[derive(Clone, PartialEq, Debug)]
enum ReductionError {
    Type,
    Io,
}

trait ResolveServiceId {
    fn resolve(&self, service_id: &TypeId) -> Option<Arc<dyn ReduceExpression>>;
}

type ReductionResult = std::result::Result<Value, ReductionError>;

async fn reduce_expression(
    argument: Value,
    service_resolver: &dyn ResolveServiceId,
    loader: &dyn LoadValue,
) -> ReductionResult {
    let service = match service_resolver.resolve(&argument.type_id) {
        Some(service) => service,
        None => return Err(ReductionError::Type),
    };
    let result = service.reduce(argument, loader).await;
    Ok(result)
}

struct ServiceRegistry {
    services: BTreeMap<TypeId, Arc<dyn ReduceExpression>>,
}

impl ResolveServiceId for ServiceRegistry {
    fn resolve(&self, service_id: &TypeId) -> Option<Arc<dyn ReduceExpression>> {
        self.services.get(service_id).cloned()
    }
}

struct TestConsole {
    writer: tokio::sync::mpsc::UnboundedSender<String>,
}

impl ReduceExpression for TestConsole {
    fn reduce(
        &self,
        mut argument: Value,
        loader: &dyn LoadValue,
    ) -> Pin<Box<dyn std::future::Future<Output = Value>>> {
        let mut arguments = argument.references.drain(0..2);
        let past_ref = arguments.next().unwrap();
        let message_ref = arguments.next().unwrap();
        assert!(arguments.next().is_none());
        let message = loader.load_value(&message_ref).unwrap();
        let message_string = message.to_string().unwrap();
        self.writer.send(message_string).unwrap();
        Box::pin(std::future::ready(make_effect(past_ref)))
    }
}

struct Identity {}

impl ReduceExpression for Identity {
    fn reduce(
        &self,
        argument: Value,
        _loader: &dyn LoadValue,
    ) -> Pin<Box<dyn std::future::Future<Output = Value>>> {
        Box::pin(std::future::ready(argument))
    }
}

fn calculate_reference(referenced: &Value) -> Reference {
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

trait StoreValue {
    fn store_value(&mut self, value: Arc<Value>) -> Reference;
}

trait LoadValue {
    fn load_value(&self, reference: &Reference) -> Option<Arc<Value>>;
}

struct InMemoryValueStorage {
    reference_to_value: BTreeMap<Reference, Arc<Value>>,
}

impl StoreValue for InMemoryValueStorage {
    fn store_value(&mut self, value: Arc<Value>) -> Reference {
        let reference = calculate_reference(&*value);
        if !self.reference_to_value.contains_key(&reference) {
            self.reference_to_value.insert(reference.clone(), value);
        }
        reference
    }
}

impl LoadValue for InMemoryValueStorage {
    fn load_value(&self, reference: &Reference) -> Option<Arc<Value>> {
        self.reference_to_value.get(reference).cloned()
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_reduce_expression() {
    let identity: Arc<dyn ReduceExpression> = Arc::new(Identity {});
    let services = ServiceRegistry {
        services: BTreeMap::from([(TypeId(0), identity.clone()), (TypeId(1), identity)]),
    };
    let value_storage = InMemoryValueStorage {
        reference_to_value: BTreeMap::new(),
    };
    let result = reduce_expression(
        Value::from_string("hello, world!\n"),
        &services,
        &value_storage,
    )
    .await
    .unwrap();
    assert_eq!(Some("hello, world!\n".to_string()), result.to_string());
}

fn make_text_in_console(past: Reference, text: Reference) -> Value {
    Value {
        type_id: TypeId(2),
        serialized: Vec::new(),
        references: vec![past, text],
    }
}

fn make_beginning_of_time() -> Value {
    Value {
        type_id: TypeId(3),
        serialized: Vec::new(),
        references: vec![],
    }
}

fn make_effect(cause: Reference) -> Value {
    Value {
        type_id: TypeId(3),
        serialized: Vec::new(),
        references: vec![cause],
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_io() {
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let test_console: Arc<dyn ReduceExpression> = Arc::new(TestConsole { writer: sender });
    let identity: Arc<dyn ReduceExpression> = Arc::new(Identity {});
    let services = ServiceRegistry {
        services: BTreeMap::from([
            (TypeId(0), identity.clone()),
            (TypeId(1), identity),
            (TypeId(2), test_console),
        ]),
    };

    let mut value_storage = InMemoryValueStorage {
        reference_to_value: BTreeMap::new(),
    };
    let past = value_storage.store_value(Arc::new(make_beginning_of_time()));
    let message = value_storage.store_value(Arc::new(Value::from_string("hello, world!\n")));
    let text_in_console = make_text_in_console(past.clone(), message);
    let result = reduce_expression(text_in_console, &services, &value_storage)
        .await
        .unwrap();
    assert_eq!(make_effect(past), result);
    assert_eq!(Some("hello, world!\n".to_string()), receiver.recv().await);
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> std::io::Result<()> {
    let mut terminal = ratatui::init();
    let period = Duration::from_secs_f32(0.2);
    let mut interval = tokio::time::interval(period);
    let mut events = crossterm::event::EventStream::new();
    let mut is_quitting = false;
    while !is_quitting {
        tokio::select! {
            _ = interval.tick() => {
                terminal.draw(|frame| {
                    let greeting = Paragraph::new("Hello World! (press 'q' to quit)");
                    frame.render_widget(greeting, frame.area());}
                )?;
                },
            Some(Ok(event)) = events.next() => {
                if let Event::Key(key) = event {
                    if KeyCode::Char('q') == key.code {
                        is_quitting=true;
                    }
                }
            },
        }
    }
    ratatui::try_restore()
}
