#[cfg(test)]
mod tests {
    use crate::file_system::DogBoxFileSystem;
    use dav_server::{fakels::FakeLs, DavHandler};
    use reqwest_dav::{Auth, ClientBuilder, Depth};
    use std::convert::Infallible;

    #[test_log::test(tokio::test)]
    async fn test_dav_access() {
        let dav_server = DavHandler::builder()
            .filesystem(Box::new(DogBoxFileSystem::new(
                dogbox_tree_editor::TreeEditor::from_entries(vec![]),
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

        let address = ([127, 0, 0, 1], 4918).into();
        let server_url = format!("http://{}", address);
        let bound = hyper::Server::bind(&address);
        let run_client = async {
            let client = ClientBuilder::new()
                .set_host(server_url)
                .set_auth(Auth::Basic("username".to_owned(), "password".to_owned()))
                .build()
                .unwrap();

            match client.get("/test.txt").await.unwrap_err() {
                reqwest_dav::Error::Reqwest(_) => panic!(),
                reqwest_dav::Error::ReqwestDecode(_) => panic!(),
                reqwest_dav::Error::MissingAuthContext => panic!(),
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
            result = bound.serve(make_service) => {
                panic!("Server isn't expected to exit: {:?}", result);
            }
            _ = run_client => {
            }
        }
    }
}
