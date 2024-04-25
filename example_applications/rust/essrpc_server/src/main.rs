use essrpc::essrpc;
use essrpc::transports::BincodeTransport;
use essrpc::RPCError;
use essrpc::RPCServer;
use std::os::fd::{FromRawFd, IntoRawFd, RawFd};

extern "C" {
    fn nonlocality_accept() -> i32;
}

#[essrpc]
pub trait Foo {
    fn bar(&self, a: String, b: i32) -> Result<String, RPCError>;
}

struct FooImpl {}

impl Foo for FooImpl {
    fn bar(&self, a: String, b: i32) -> Result<String, RPCError> {
        println!("Hello, world!");
        Ok("12345".to_string())
    }
}

fn main() {
    println!("Accepting an API client..");
    let api_fd = unsafe { nonlocality_accept() };
    println!("Accepted an API client..");
    let file = unsafe { std::fs::File::from_raw_fd(api_fd) };
    let mut s = FooRPCServer::new(FooImpl {}, BincodeTransport::new(file));
    match s.serve() {
        Ok(()) => {
            println!("Serve completed successfully.");
        }
        Err(error) => {
            println!("Serve failed: {}.", error);
        }
    }
}
