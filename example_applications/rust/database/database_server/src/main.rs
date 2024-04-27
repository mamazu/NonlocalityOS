use database_trait::Foo;
use database_trait::FooRPCServer;
use essrpc::transports::BincodeTransport;
use essrpc::RPCError;
use essrpc::RPCServer;
use nonlocality_env::accept;
use sqlite::State;

struct FooImpl {}

impl Foo for FooImpl {
    fn bar(&self, a: String, b: i32) -> Result<String, RPCError> {
        println!("Hello, world!");
        Ok("12345".to_string())
    }
}

fn main() {
    let connection = sqlite::open(":memory:").unwrap();

    let query = "
    CREATE TABLE users (name TEXT, age INTEGER);
    INSERT INTO users VALUES ('Alice', 42);
    INSERT INTO users VALUES ('Bob', 69);";
    connection.execute(query).unwrap();

    let query = "SELECT * FROM users WHERE age > ?";
    let mut statement = connection.prepare(query).unwrap();
    statement.bind((1, 50)).unwrap();

    while let Ok(State::Row) = statement.next() {
        println!("name = {}", statement.read::<String, _>("name").unwrap());
        println!("age = {}", statement.read::<i64, _>("age").unwrap());
    }

    println!("Accepting an API client..");
    let accepted = accept();
    println!(
        "Accepted an API client for interface {}.",
        accepted.interface
    );
    let mut server = FooRPCServer::new(FooImpl {}, BincodeTransport::new(accepted.stream));
    match server.serve() {
        Ok(()) => {
            println!("Serve completed successfully.");
        }
        Err(error) => {
            println!("Serve failed: {}.", error);
        }
    }
}
