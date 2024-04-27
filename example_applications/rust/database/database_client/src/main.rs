use database_trait::Foo;
use database_trait::FooRPCClient;
use essrpc::transports::BincodeTransport;
use essrpc::RPCClient;
use nonlocality_env::nonlocality_connect;
use std::os::fd::{FromRawFd, IntoRawFd, RawFd};

fn main() {
    println!("Connecting to an API..");
    let api_fd = unsafe { nonlocality_connect(0) };
    println!("Connected to an API..");
    let file = unsafe { std::fs::File::from_raw_fd(api_fd) };
    let client = FooRPCClient::new(BincodeTransport::new(file));
    match client.bar("the answer".to_string(), 42) {
        Ok(result) => assert_eq!("12345", result),
        Err(e) => panic!("error: {:?}", e),
    }
}
