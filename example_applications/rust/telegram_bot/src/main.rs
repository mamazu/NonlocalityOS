#[deny(warnings)]
use frankenstein::GetUpdatesParams;
use frankenstein::ReplyParameters;
use frankenstein::SendMessageParams;
use frankenstein::TelegramApi;
use frankenstein::{Api, UpdateContent};
use std::net::SocketAddr;
use std::net::SocketAddrV4;
use std::str::FromStr;
use std::sync::Arc;
use ureq::TlsConnector;

static TOKEN: &str = "API_TOKEN";

struct HttpsBridge {}

impl TlsConnector for HttpsBridge {
    fn connect(
        &self,
        dns_name: &str,
        io: Box<dyn ureq::ReadWrite>,
    ) -> Result<Box<dyn ureq::ReadWrite>, ureq::Error> {
        todo!()
    }
}

fn main() {
    let https = Arc::new(HttpsBridge {});
    let mut api = Api::new(TOKEN);
    api.request_agent = ureq::AgentBuilder::new()
        .resolver(|name: &str| -> std::io::Result<Vec<SocketAddr>> {
            println!("TEST");
            Ok(vec!["149.154.167.220:443".parse().unwrap()])
        })
        //.tls_connector(https)
        .build();

    let update_params_builder = GetUpdatesParams::builder();
    let mut update_params = update_params_builder.clone().build();

    loop {
        let result = api.get_updates(&update_params);

        println!("result: {result:?}");

        match result {
            Ok(response) => {
                for update in response.result {
                    if let UpdateContent::Message(message) = update.content {
                        let reply_parameters = ReplyParameters::builder()
                            .message_id(message.message_id)
                            .build();

                        let send_message_params = SendMessageParams::builder()
                            .chat_id(message.chat.id)
                            .text("hello")
                            .reply_parameters(reply_parameters)
                            .build();

                        if let Err(err) = api.send_message(&send_message_params) {
                            println!("Failed to send message: {err:?}");
                        }
                    }
                    update_params = update_params_builder
                        .clone()
                        .offset(update.update_id + 1)
                        .build();
                }
            }
            Err(error) => {
                println!("Failed to get updates: {error:?}");
                break;
            }
        }
    }
}
