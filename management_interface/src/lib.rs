use essrpc::essrpc;
use essrpc::RPCError;

#[essrpc]
pub trait ManagementInterface {
    fn shutdown(&self) -> Result<bool, RPCError>;
}
