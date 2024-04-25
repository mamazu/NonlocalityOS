use essrpc::essrpc;
use essrpc::transports::BincodeTransport;
use essrpc::RPCError;
use essrpc::RPCServer;
use nonlocality_env::accept;
use std::os::fd::{FromRawFd, IntoRawFd, RawFd};
use std::sync::atomic::AtomicBool;

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
    let is_done = std::sync::Arc::new(AtomicBool::new(false));

    // Rust is unnecessarily complicated sometimes
    let is_done_closure = is_done.clone();

    let background_acceptor = std::thread::spawn(move || {
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
        is_done_closure.store(true, std::sync::atomic::Ordering::SeqCst);
        println!("Background thread exiting");
    });
    while !is_done.load(std::sync::atomic::Ordering::SeqCst) {
        println!("Main thread waiting..");
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    println!("Main thread joining the background thread");
    background_acceptor.join();
    println!("Main thread exiting");
}