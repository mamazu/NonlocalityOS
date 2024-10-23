use astraea::storage::{LoadRoot, SQLiteStorage, UpdateRoot};
use dav_server::{fakels::FakeLs, DavHandler};
use dogbox_tree_editor::OpenDirectory;
use hyper::{body, server::conn::http1, Request};
use hyper_util::rt::TokioIo;
use std::{
    convert::Infallible,
    net::SocketAddr,
    path::Path,
    sync::{Arc, Mutex},
};
use tokio::net::{TcpListener, TcpStream};
mod file_system;
mod file_system_test;
use file_system::DogBoxFileSystem;

async fn serve_connection(stream: TcpStream, dav_server: Arc<DavHandler>) {
    let make_service = move |req: Request<body::Incoming>| {
        let dav_server = dav_server.clone();
        async move { Ok::<_, Infallible>(dav_server.handle(req).await) }
    };
    let io = TokioIo::new(stream);
    if let Err(err) = http1::Builder::new()
        .serve_connection(io, hyper::service::service_fn(make_service))
        .await
    {
        eprintln!("Error serving connection: {:?}", err);
    }
}

async fn handle_tcp_connections(
    listener: TcpListener,
    dav_server: Arc<DavHandler>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    loop {
        let (stream, _) = listener.accept().await?;
        let dav_server = dav_server.clone();
        tokio::task::spawn(async move { serve_connection(stream, dav_server).await });
    }
}

async fn save_tree_regularly(
    root: Arc<OpenDirectory>,
    root_name: &str,
    blob_storage: &(dyn UpdateRoot + Sync),
) {
    let mut previous_root_digest = None;
    loop {
        tokio::time::sleep(std::time::Duration::from_secs(1)).await;
        match root.poll_status().await {
            Ok(root_digest) => {
                if previous_root_digest != Some(root_digest) {
                    println!("Root digest changed: {:?}", &root_digest);
                    blob_storage.update_root(root_name, &root_digest);
                    previous_root_digest = Some(root_digest);
                }
            }
            Err(error) => {
                println!("Could not poll root status: {:?}", &error);
            }
        }
    }
}

async fn run_dav_server(
    listener: TcpListener,
    database_file_name: &Path,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let database_existed = std::fs::exists(&database_file_name).unwrap();
    let sqlite_connection = rusqlite::Connection::open(&database_file_name)?;
    if !database_existed {
        match SQLiteStorage::create_schema(&sqlite_connection) {
            Ok(_) => {}
            Err(error) => {
                println!(
                    "Could not create SQL schema in {}: {:?}",
                    &database_file_name.display(),
                    &error
                );
                println!("Deleting {}", &database_file_name.display());
                std::fs::remove_file(&database_file_name).unwrap();
                panic!();
            }
        }
    }
    let blob_storage = Arc::new(SQLiteStorage::new(Mutex::new(sqlite_connection)));
    let root_name = "latest";
    let root = match blob_storage.load_root(&root_name) {
        Some(found) => {
            OpenDirectory::load_directory(blob_storage.clone(), &found).await.unwrap(/*TODO*/)
        }
        None => Arc::new(OpenDirectory::from_entries(vec![], blob_storage.clone())),
    };
    let dav_server = Arc::new(
        DavHandler::builder()
            .filesystem(Box::new(DogBoxFileSystem::new(
                dogbox_tree_editor::TreeEditor::new(root.clone()),
            )))
            .locksystem(FakeLs::new())
            .build_handler(),
    );
    let (_, result) = tokio::join!(
        async move {
            save_tree_regularly(root, &root_name, &*blob_storage).await;
        },
        handle_tcp_connections(listener, dav_server)
    );
    result
}

#[cfg(test)]
mod tests {
    use crate::run_dav_server;
    use reqwest_dav::{list_cmd::ListEntity, Auth, Client, ClientBuilder, Depth};
    use std::{future::Future, net::SocketAddr, pin::Pin};
    use tokio::net::TcpListener;

    async fn test_fresh_dav_server(
        run_client: impl FnOnce(String) -> Pin<Box<dyn Future<Output = ()>>>,
    ) {
        let address = SocketAddr::from(([127, 0, 0, 1], 0));
        let listener = TcpListener::bind(address).await.unwrap();
        let actual_address = listener.local_addr().unwrap();
        let temporary_directory = tempfile::tempdir().unwrap();
        let database_file_name = temporary_directory.path().join("dogbox_dav_server.sqlite");
        let server_url = format!("http://{}", actual_address);
        tokio::select! {
            result = run_dav_server(listener, &database_file_name ) => {
                panic!("Server isn't expected to exit: {:?}", result);
            }
            _ = run_client(server_url) => {
            }
        }
    }

    fn create_client(server_url: String) -> Client {
        let client = ClientBuilder::new()
            .set_host(server_url)
            .set_auth(Auth::Basic("username".to_owned(), "password".to_owned()))
            .build()
            .unwrap();
        client
    }

    fn expect_directory(entity: &ListEntity, name: &str) {
        match entity {
            reqwest_dav::list_cmd::ListEntity::File(_) => panic!(),
            reqwest_dav::list_cmd::ListEntity::Folder(folder) => {
                assert_eq!(name, folder.href);
                assert_eq!(None, folder.quota_used_bytes);
                assert_eq!(None, folder.quota_available_bytes);
                //TODO: check tag value
                assert_eq!(true, folder.tag.is_some());
                //TODO: check last modified
            }
        }
    }

    fn expect_file(entity: &ListEntity, name: &str, size: i64) {
        match entity {
            reqwest_dav::list_cmd::ListEntity::File(file) => {
                assert_eq!(name, file.href);
                assert_eq!(size, file.content_length);
                assert_eq!("text/plain", file.content_type);
                //TODO: check tag value
                assert_eq!(true, file.tag.is_some());
                //TODO: check last modified
            }
            reqwest_dav::list_cmd::ListEntity::Folder(_folder) => panic!(),
        }
    }

    #[test_log::test(tokio::test)]
    async fn test_file_not_found() {
        let run_client = |server_url| -> Pin<Box<dyn Future<Output = ()>>> {
            Box::pin(async {
                let client = create_client(server_url);
                let error = client.get("/test.txt").await.unwrap_err();
                match error {
                    reqwest_dav::Error::Reqwest(_) | reqwest_dav::Error::ReqwestDecode(_) | reqwest_dav::Error::MissingAuthContext => panic!("Unexpected error: {:?}", &error),
                    reqwest_dav::Error::Decode(decode) => match decode {
                        reqwest_dav::DecodeError::DigestAuth(_) => panic!(),
                        reqwest_dav::DecodeError::NoAuthHeaderInResponse => panic!(),
                        reqwest_dav::DecodeError::SerdeXml(_) => panic!(),
                        reqwest_dav::DecodeError::FieldNotSupported(_) => panic!(),
                        reqwest_dav::DecodeError::FieldNotFound(_) => panic!(),
                        reqwest_dav::DecodeError::StatusMismatched(_) => panic!(),
                        reqwest_dav::DecodeError::Server(server_error) =>
                            assert_eq!("ServerError { response_code: 404, exception: \"server exception and parse error\", message: \"\" }",
                                format!("{:?}", server_error)),
                    },
                };
                let listed = client.list("", Depth::Number(1)).await.unwrap();
                assert_eq!(1, listed.len());
                expect_directory(&listed[0], "/");
            })
        };
        test_fresh_dav_server(run_client).await
    }

    async fn test_create_file(content: Vec<u8>) {
        let run_client = move |server_url| -> Pin<Box<dyn Future<Output = ()>>> {
            Box::pin(async move {
                let client = create_client(server_url);
                let size = content.len() as i64;
                let file_name = "test.txt";
                client.put(file_name, content.clone()).await.unwrap();
                let listed = client.list("", Depth::Number(1)).await.unwrap();
                assert_eq!(2, listed.len());
                expect_directory(&listed[0], "/");
                expect_file(&listed[1], &format!("/{}", file_name), size);
                let response = client.get(&file_name).await.unwrap();
                let response_content = response.bytes().await.unwrap().to_vec();
                assert_eq!(content, response_content);
            })
        };
        test_fresh_dav_server(run_client).await
    }

    #[test_log::test(tokio::test)]
    async fn test_create_file_empty() {
        test_create_file(vec![]).await
    }

    #[test_log::test(tokio::test)]
    async fn test_create_file_with_content() {
        test_create_file(vec![b'a']).await
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt::init();
    let address = SocketAddr::from(([127, 0, 0, 1], 4918));
    let database_file_name = std::env::current_dir()
        .unwrap()
        .join("dogbox_dav_server.sqlite");
    let listener = TcpListener::bind(address).await?;
    println!("Serving on http://{}", address);
    run_dav_server(listener, &database_file_name).await
}
