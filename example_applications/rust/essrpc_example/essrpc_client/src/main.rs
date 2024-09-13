use essrpc::transports::BincodeTransport;
use essrpc::RPCClient;
use essrpc_trait::Foo;
use essrpc_trait::FooRPCClient;
use nonlocality_env::connect;

fn main() {
    println!("Connecting to an API..");
    let connected = connect(0);
    println!("Connected to an API..");
    let client = FooRPCClient::new(BincodeTransport::new(connected));
    match client.bar("the answer".to_string(), 42) {
        Ok(result) => assert_eq!("12345", result),
        Err(e) => panic!("error: {:?}", e),
    }
}
