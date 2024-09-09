use futures::StreamExt;
use ratatui::{
    crossterm::{
        self,
        event::{Event, KeyCode},
    },
    widgets::Paragraph,
};
use std::{collections::BTreeMap, pin::Pin, sync::Arc, time::Duration};

#[derive(Clone, PartialEq, Debug)]
struct TypeId(u64);

#[derive(Clone, PartialEq, Debug)]
struct Value {
    type_id: TypeId,
    serialized: Vec<u8>,
}

impl Value {
    fn from_string(value: &str) -> Value {
        Value {
            type_id: TypeId(0),
            serialized: value.as_bytes().to_vec(),
        }
    }

    fn from_unit() -> Value {
        Value {
            type_id: TypeId(1),
            serialized: Vec::new(),
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

#[derive(Clone, PartialEq, PartialOrd, Ord, Eq)]
struct ServiceId(u64);

#[derive(Clone)]
enum Expression {
    Atom(Value),
    Call {
        service: ServiceId,
        argument: Arc<Expression>,
        result_type_id: TypeId,
    },
}

trait ReduceExpression: Sync + Send {
    fn reduce(&self, argument: Value) -> Pin<Box<dyn std::future::Future<Output = Value>>>;
}

#[derive(Clone, PartialEq, Debug)]
enum EvaluationResult {
    Success(Value),
    TypeError,
    IoError,
}

trait ResolveServiceId {
    fn resolve(&self, service_id: &ServiceId) -> Option<Arc<dyn ReduceExpression>>;
}

async fn evaluate_call_expression(
    service_id: &ServiceId,
    argument: &Arc<Expression>,
    result_type_id: &TypeId,
    service_resolver: &dyn ResolveServiceId,
) -> EvaluationResult {
    let recursion = Box::pin(evaluate_expression(argument.as_ref(), service_resolver));
    let argument_evaluated = match recursion.await {
        EvaluationResult::Success(success) => success,
        EvaluationResult::TypeError => return EvaluationResult::TypeError,
        EvaluationResult::IoError => return EvaluationResult::IoError,
    };
    let service = match service_resolver.resolve(service_id) {
        Some(service) => service,
        None => return EvaluationResult::TypeError,
    };
    let result = service.reduce(argument_evaluated).await;
    if result.type_id != *result_type_id {
        return EvaluationResult::TypeError;
    }
    EvaluationResult::Success(result)
}

struct ServiceRegistry {
    services: BTreeMap<ServiceId, Arc<dyn ReduceExpression>>,
}

impl ResolveServiceId for ServiceRegistry {
    fn resolve(&self, service_id: &ServiceId) -> Option<Arc<dyn ReduceExpression>> {
        self.services.get(service_id).cloned()
    }
}

struct TestConsole {
    writer: tokio::sync::mpsc::UnboundedSender<String>,
}

impl ReduceExpression for TestConsole {
    fn reduce(&self, argument: Value) -> Pin<Box<dyn std::future::Future<Output = Value>>> {
        let message = argument.to_string().unwrap();
        self.writer.send(message).unwrap();
        Box::pin(std::future::ready(Value::from_unit()))
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_evaluate_call_expression() {
    let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
    let test_console: Arc<dyn ReduceExpression> = Arc::new(TestConsole { writer: sender });
    let services = ServiceRegistry {
        services: BTreeMap::from([(ServiceId(0), test_console)]),
    };
    let expected_result = Value::from_unit();
    let result = evaluate_call_expression(
        &ServiceId(0),
        &Arc::new(Expression::Atom(Value::from_string("hello, world!\n"))),
        &expected_result.type_id,
        &services,
    )
    .await;
    assert_eq!(EvaluationResult::Success(expected_result), result);
    assert_eq!(Some("hello, world!\n".to_string()), receiver.recv().await);
}

async fn evaluate_expression(
    evaluating: &Expression,
    service_resolver: &dyn ResolveServiceId,
) -> EvaluationResult {
    match evaluating {
        Expression::Atom(value) => EvaluationResult::Success(value.clone()),
        Expression::Call {
            service,
            argument,
            result_type_id,
        } => evaluate_call_expression(service, argument, result_type_id, service_resolver).await,
    }
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
