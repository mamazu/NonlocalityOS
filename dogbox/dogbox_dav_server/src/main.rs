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
    use reqwest_dav::{Auth, ClientBuilder, Depth};
    use std::net::SocketAddr;
    use tokio::net::TcpListener;

    #[test_log::test(tokio::test)]
    async fn test_dav_server() {
        let address = SocketAddr::from(([127, 0, 0, 1], 4919));
        let listener = TcpListener::bind(address).await.unwrap();
        let temporary_directory = tempfile::tempdir().unwrap();
        let database_file_name = temporary_directory.path().join("dogbox_dav_server.sqlite");
        let server_url = format!("http://{}", address);
        let run_client = async {
            let client = ClientBuilder::new()
                .set_host(server_url)
                .set_auth(Auth::Basic("username".to_owned(), "password".to_owned()))
                .build()
                .unwrap();
            let error = client.get("/test.txt").await.unwrap_err();
            match error {
            reqwest_dav::Error::Reqwest(_)| reqwest_dav::Error::ReqwestDecode(_)| reqwest_dav::Error::MissingAuthContext => panic!("Unexpected error: {:?}", &error),
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

            let listed = client.list("", Depth::Number(0)).await.unwrap();
            assert_eq!(1, listed.len());
            let entry = &listed[0];
            match entry {
                reqwest_dav::list_cmd::ListEntity::File(_) => panic!(),
                reqwest_dav::list_cmd::ListEntity::Folder(folder) => {
                    assert_eq!("/", folder.href);
                    assert_eq!(None, folder.quota_used_bytes);
                    assert_eq!(None, folder.quota_available_bytes);
                    //TODO: check tag value
                    assert_eq!(true, folder.tag.is_some());
                    //TODO: check last modified
                }
            }
        };
        tokio::select! {
            result = run_dav_server(listener, &database_file_name ) => {
                panic!("Server isn't expected to exit: {:?}", result);
            }
            _ = run_client => {
            }
        }
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
