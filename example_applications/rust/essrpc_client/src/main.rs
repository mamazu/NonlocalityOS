use essrpc::essrpc;
use essrpc::transports::BincodeTransport;
use essrpc::RPCClient;
use essrpc::RPCError;
use std::os::fd::{FromRawFd, IntoRawFd, RawFd};

extern "C" {
    fn nonlocality_connect() -> i32;
}

#[essrpc]
pub trait Foo {
    fn bar(&self, a: String, b: i32) -> Result<String, RPCError>;
}

fn main() {
    println!("Connecting to an API..");
    let api_fd = unsafe { nonlocality_connect() };
    println!("Connected to an API..");
    let file = unsafe { std::fs::File::from_raw_fd(api_fd) };
    let client = FooRPCClient::new(BincodeTransport::new(file));
    match client.bar("the answer".to_string(), 42) {
        Ok(result) => assert_eq!("12345", result),
        Err(e) => panic!("error: {:?}", e),
    }
}
