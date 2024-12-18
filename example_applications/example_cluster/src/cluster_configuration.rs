use management_interface::Blob;
use nonlocality_build_utils::wasi::WASIP1_TARGET;
use nonlocality_build_utils::wasi::WASIP1_THREADS_TARGET;
use std::collections::BTreeMap;
use tokio::io::AsyncReadExt;

use management_interface::IncomingInterface;
use management_interface::IncomingInterfaceId;
use management_interface::OutgoingInterfaceId;
use management_interface::ServiceId;
use management_interface::{ClusterConfiguration, Service, WasiProcess};

async fn read_blob(from: &std::path::Path) -> Blob {
    let mut file = tokio::fs::File::open(from)
        .await
        .expect(&format!("Tried to open {}", from.display()));
    let mut contents = vec![];
    file.read_to_end(&mut contents).await.unwrap();
    Blob::Direct(contents)
}

pub async fn compile_cluster_configuration(target: &std::path::Path) -> ClusterConfiguration {
    let hello_world_id = ServiceId(0);
    let essrpc_server_id = ServiceId(1);
    let essrpc_client_id = ServiceId(2);
    let provide_api_id = ServiceId(3);
    let call_api_id = ServiceId(4);
    let idle_service_id = ServiceId(7);
    let telegram_bot_id = ServiceId(10);

    ClusterConfiguration {
        services: vec![
            Service {
                id: hello_world_id,
                label: "Hello world service".to_string(),
                outgoing_interfaces: BTreeMap::new(),
                wasi: WasiProcess {
                    code: read_blob(&target.join(WASIP1_TARGET).join("release/hello_rust.wasm"))
                        .await,
                    has_threads: false,
                },
                filesystem_dir_unique_id: None,
            },
            Service {
                id: essrpc_server_id,
                label: "ESS RPC Server".to_string(),
                outgoing_interfaces: BTreeMap::new(),
                wasi: WasiProcess {
                    code: read_blob(
                        &target
                            .join(WASIP1_THREADS_TARGET)
                            .join("release/essrpc_server.wasm"),
                    )
                    .await,
                    has_threads: true,
                },
                filesystem_dir_unique_id: None,
            },
            Service {
                id: essrpc_client_id,
                label: "ESS RPC Client".to_string(),
                outgoing_interfaces: BTreeMap::from([(
                    OutgoingInterfaceId(0),
                    IncomingInterface::new(essrpc_server_id, IncomingInterfaceId(0)),
                )]),
                wasi: WasiProcess {
                    code: read_blob(
                        &target
                            .join(WASIP1_TARGET)
                            .join("release/essrpc_client.wasm"),
                    )
                    .await,
                    has_threads: false,
                },
                filesystem_dir_unique_id: None,
            },
            Service {
                id: provide_api_id,
                label: "Provide API".to_string(),
                outgoing_interfaces: BTreeMap::new(),
                wasi: WasiProcess {
                    code: read_blob(&target.join(WASIP1_TARGET).join("release/provide_api.wasm"))
                        .await,
                    has_threads: false,
                },
                filesystem_dir_unique_id: None,
            },
            Service {
                id: call_api_id,
                label: "Call API".to_string(),
                outgoing_interfaces: BTreeMap::from([(
                    OutgoingInterfaceId(0),
                    IncomingInterface::new(provide_api_id, IncomingInterfaceId(0)),
                )]),
                wasi: WasiProcess {
                    code: read_blob(&target.join(WASIP1_TARGET).join("release/call_api.wasm"))
                        .await,
                    has_threads: false,
                },
                filesystem_dir_unique_id: None,
            },
            Service {
                id: idle_service_id,
                label: "Idle Service".to_string(),
                outgoing_interfaces: BTreeMap::new(),
                wasi: WasiProcess {
                    code: read_blob(&target.join(WASIP1_TARGET).join("release/idle_service.wasm"))
                        .await,
                    has_threads: false,
                },
                filesystem_dir_unique_id: None,
            },
            Service {
                id: telegram_bot_id,
                label: "Telegram Bot".to_string(),
                outgoing_interfaces: BTreeMap::from([]),
                wasi: WasiProcess {
                    code: read_blob(
                        &target
                            .join(WASIP1_THREADS_TARGET)
                            .join("release/telegram_bot.wasm"),
                    )
                    .await,
                    has_threads: true,
                },
                filesystem_dir_unique_id: None,
            },
        ],
    }
}
