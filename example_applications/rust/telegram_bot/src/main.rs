#[deny(warnings)]
use nonlocality_env::tcp_ssl_handshake;
use std::io::Read;

fn main() {
    let mut connection = tcp_ssl_handshake("api.telegram.org", 443)
        .expect("SSL connect/handshake with Telegram API");
    let mut received = String::new();
    connection.read_to_string(&mut received).expect("Receive");
}
