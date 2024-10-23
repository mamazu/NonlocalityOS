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
    // Finally, we bind the incoming connection to our `hello` service
    if let Err(err) = http1::Builder::new()
        // `service_fn` converts our function in a `Service`
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
    let addr = SocketAddr::from(([127, 0, 0, 1], 4918));
    let listener = TcpListener::bind(addr).await?;
    println!("Serving on http://{}", addr);
    let (_, result) = tokio::join!(
        async move {
            save_tree_regularly(root, &root_name, &*blob_storage).await;
        },
        handle_tcp_connections(listener, dav_server)
    );
    result
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt::init();
    let database_file_name = std::env::current_dir()
        .unwrap()
        .join("dogbox_dav_server.sqlite");
    run_dav_server(&database_file_name).await
}
