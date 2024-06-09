use dav_server::{fakels::FakeLs, DavHandler};
use dogbox_tree_editor::{DirectoryEntry, DirectoryEntryKind};
use std::convert::Infallible;
mod file_system;
use file_system::DogBoxFileSystem;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let dav_server = DavHandler::builder()
        .filesystem(Box::new(DogBoxFileSystem::new(
            dogbox_tree_editor::TreeEditor::from_entries(vec![DirectoryEntry {
                name: "example.txt".to_string(),
                kind: DirectoryEntryKind::File(0),
            }]),
        )))
        .locksystem(FakeLs::new())
        .build_handler();

    let make_service = hyper::service::make_service_fn(move |_| {
        let dav_server = dav_server.clone();
        async move {
            let func = move |req| {
                let dav_server = dav_server.clone();
                async move { Ok::<_, Infallible>(dav_server.handle(req).await) }
            };
            Ok::<_, Infallible>(hyper::service::service_fn(func))
        }
    });

    let addr = ([127, 0, 0, 1], 4918).into();
    println!("Serving on http://{}", addr);
    let _ = hyper::Server::bind(&addr)
        .serve(make_service)
        .await
        .map_err(|e| eprintln!("server error: {}", e));
}
