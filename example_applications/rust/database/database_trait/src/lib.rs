use essrpc::essrpc;
use essrpc::RPCError;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
pub struct User {
    pub name: String,
    pub role: String,
}

#[essrpc]
pub trait ExampleDatabase {
    fn create_user(&self, name: String, role: String) -> Result<bool, RPCError>;
    fn list_users(&self) -> Result<Vec<User>, RPCError>;
}
