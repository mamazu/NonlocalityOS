use astraea::{storage::SQLiteStorage, tree::BlobDigest};
use dav_server::{fakels::FakeLs, DavHandler};
use dogbox_tree_editor::{DirectoryEntry, DirectoryEntryKind, OpenDirectory};
use hyper::{body, server::conn::http1, Request};
use hyper_util::rt::TokioIo;
use std::{
    convert::Infallible,
    net::SocketAddr,
    sync::{Arc, Mutex},
};
use tokio::net::TcpListener;
mod file_system;
mod file_system_test;
use file_system::DogBoxFileSystem;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    tracing_subscriber::fmt::init();

    let database_file_name = std::env::current_dir()
        .unwrap()
        .join("dogbox_dav_server.sqlite");
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
    let root = Arc::new(OpenDirectory::from_entries(
        vec![DirectoryEntry {
            name: "example.txt".to_string(),
            kind: DirectoryEntryKind::File(0),
            digest: BlobDigest::hash(b""),
        }],
        blob_storage,
    ));
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

    tokio::spawn(async move {
        let mut previous_root_digest = None;
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            match root.poll_status().await {
                Ok(root_digest) => {
                    if previous_root_digest != Some(root_digest) {
                        println!("Root digest changed: {:?}", &root_digest);
                        previous_root_digest = Some(root_digest);
                    }
                }
                Err(error) => {
                    println!("Could not poll root status: {:?}", &error);
                }
            }
        }
    });

    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);
        let dav_server = dav_server.clone();
        tokio::task::spawn(async move {
            let make_service = move |req: Request<body::Incoming>| {
                let dav_server = dav_server.clone();
                async move { Ok::<_, Infallible>(dav_server.handle(req).await) }
            };

            // Finally, we bind the incoming connection to our `hello` service
            if let Err(err) = http1::Builder::new()
                // `service_fn` converts our function in a `Service`
                .serve_connection(io, hyper::service::service_fn(make_service))
                .await
            {
                eprintln!("Error serving connection: {:?}", err);
            }
        });
    }
}
