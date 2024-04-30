use essrpc::essrpc;
use essrpc::RPCError;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, PartialEq, PartialOrd, Ord, Eq, Clone, Copy)]
pub struct BlobDigest(pub ([u8; 32], [u8; 32]));

#[derive(Serialize, Deserialize, Debug, PartialEq, PartialOrd, Ord, Eq, Clone, Copy)]
pub struct IncomingInterfaceId(pub i32);

#[derive(Serialize, Deserialize, Debug, PartialEq, PartialOrd, Ord, Eq, Clone, Copy)]
pub struct OutgoingInterfaceId(pub i32);

#[derive(Serialize, Deserialize, Debug, PartialEq, PartialOrd, Ord, Eq, Clone, Copy)]
pub struct ServiceId(pub i32);

#[derive(Serialize, Deserialize, Debug, PartialEq, PartialOrd, Ord, Eq, Clone, Copy)]
pub struct WasiProcess {
    code: BlobDigest,
    has_threads: bool,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, PartialOrd, Ord, Eq, Clone, Copy)]
pub struct IncomingInterface {
    destination_service: ServiceId,
    interface: IncomingInterfaceId,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, PartialOrd, Ord, Eq, Clone)]
pub struct Service {
    id: ServiceId,
    outgoing_interfaces: std::collections::BTreeMap<OutgoingInterfaceId, IncomingInterface>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, PartialOrd, Ord, Eq, Clone)]
pub struct ClusterConfiguration {
    services: Vec<Service>,
}

#[derive(Serialize, Deserialize, Debug, PartialEq, PartialOrd, Ord, Eq, Clone, Copy)]
pub enum ConfigurationError {
    NotImplemented,
}

#[essrpc]
pub trait ManagementInterface {
    fn shutdown(&self) -> Result<bool, RPCError>;
    fn reconfigure(
        &self,
        configuration: ClusterConfiguration,
    ) -> Result<Option<ConfigurationError>, RPCError>;
}
