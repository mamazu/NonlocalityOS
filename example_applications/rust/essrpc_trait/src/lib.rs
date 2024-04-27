use essrpc::essrpc;
use essrpc::RPCError;

#[essrpc]
pub trait Foo {
    fn bar(&self, a: String, b: i32) -> Result<String, RPCError>;
}
