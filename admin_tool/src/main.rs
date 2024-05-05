#[deny(warnings)]
use essrpc::transports::BincodeTransport;
use essrpc::RPCClient;
use management_interface::ManagementInterface;
use management_interface::ManagementInterfaceRPCClient;

fn main() -> std::io::Result<()> {
    println!("Connecting..");
    let connector = std::net::TcpStream::connect("127.0.0.1:6969")?;
    println!("Connected.");
    let client = ManagementInterfaceRPCClient::new(BincodeTransport::new(connector));
    match client.shutdown() {
        Ok(is_success) => {
            if is_success {
                println!("Shutdown confirmed.");
            } else {
                println!("Shutdown not possible.");
            }
        }
        Err(error) => {
            println!("Shutdown failed with {}.", error);
        }
    }
    Ok(())
}
