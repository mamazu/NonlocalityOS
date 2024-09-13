use database_trait::ExampleDatabase;
use database_trait::ExampleDatabaseRPCClient;
use essrpc::transports::BincodeTransport;
use essrpc::RPCClient;
use nonlocality_env::connect;

fn main() {
    println!("Connecting to an API..");
    let connected = connect(0);
    println!("Connected to an API..");
    let client = ExampleDatabaseRPCClient::new(BincodeTransport::new(connected));
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
