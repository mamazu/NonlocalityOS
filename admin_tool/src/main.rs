use essrpc::transports::BincodeTransport;
use essrpc::RPCClient;
use management_interface::ManagementInterface;
use management_interface::ManagementInterfaceRPCClient;

fn main() -> std::io::Result<()> {
    let args: Vec<String> = std::env::args().collect();
    println!("Connecting..");
    let connector = std::net::TcpStream::connect("127.0.0.1:6969")?;
    println!("Connected.");
    let client = ManagementInterfaceRPCClient::new(BincodeTransport::new(connector));
    let command = args[1].as_str();
    match command {
        "shutdown" => {
            println!("Sending shutdown request.");
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
        "reconfigure" => {
            let cluster_configuration_file_path = std::path::Path::new(&args[2]);
            println!(
                "Loading configuration from {}",
                cluster_configuration_file_path.display()
            );
            let cluster_configuration_content =
                std::fs::read(&cluster_configuration_file_path).unwrap();
            let cluster_configuration =
                postcard::from_bytes(&cluster_configuration_content[..]).unwrap();
            println!("Sending new configuration.");
            match client.reconfigure(cluster_configuration) {
                Ok(maybe_error) => match maybe_error {
                    Some(error) => println!("Reconfiguration not possible: {:?}", error),
                    None => println!("Reconfiguration confirmed."),
                },
                Err(error) => {
                    println!("Reconfiguration failed with {}.", error);
                }
            }
            Ok(())
        }
        _ => {
            panic!("Unknown command: {}", command);
        }
    }
}
