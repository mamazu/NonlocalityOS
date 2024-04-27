use database_trait::ExampleDatabase;
use database_trait::ExampleDatabaseRPCClient;
use essrpc::transports::BincodeTransport;
use essrpc::RPCClient;
use nonlocality_env::nonlocality_connect;
use std::os::fd::{FromRawFd, IntoRawFd, RawFd};

fn main() {
    println!("Connecting to an API..");
    let api_fd = unsafe { nonlocality_connect(0) };
    println!("Connected to an API..");
    let file = unsafe { std::fs::File::from_raw_fd(api_fd) };
    let client = ExampleDatabaseRPCClient::new(BincodeTransport::new(file));
    match client.create_user("Alice".to_string(), "admin".to_string()) {
        Ok(is_created) => assert_eq!(true, is_created),
        Err(e) => panic!("error: {:?}", e),
    }
    match client.list_users() {
        Ok(users) => {
            for user in users {
                println!("User: {}, role {}.", user.name, user.role);
            }
        }
        Err(e) => panic!("error: {:?}", e),
    }
}
