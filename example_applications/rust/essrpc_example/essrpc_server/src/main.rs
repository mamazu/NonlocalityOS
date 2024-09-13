use essrpc::transports::BincodeTransport;
use essrpc::RPCError;
use essrpc::RPCServer;
use essrpc_trait::Foo;
use essrpc_trait::FooRPCServer;
use nonlocality_env::accept;
use std::sync::atomic::AtomicBool;

struct FooImpl {}

impl Foo for FooImpl {
    fn bar(&self, a: String, b: i32) -> Result<String, RPCError> {
        println!("Hello, world! {} {}", a, b);
        Ok("12345".to_string())
    }
}

fn main() -> std::io::Result<()> {
    let is_done = std::sync::Arc::new(AtomicBool::new(false));

    // Rust is unnecessarily complicated sometimes
    let is_done_closure = is_done.clone();

    let background_acceptor = std::thread::spawn(move || -> std::io::Result<()> {
        println!("Accepting an API client..");
        let accepted = accept()?;
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
        Ok(())
    });
    while !is_done.load(std::sync::atomic::Ordering::SeqCst) {
        println!("Main thread waiting..");
        std::thread::sleep(std::time::Duration::from_millis(1000));
    }
    println!("Main thread joining the background thread");
    background_acceptor.join().unwrap()?;
    println!("Main thread exiting");
    Ok(())
}
