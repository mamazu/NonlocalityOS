use nonlocality_env::tcp_ssl_handshake;
use std::io::Read;
use std::io::Write;

fn main() {
    println!("Create SSL stream");
    let mut connection = tcp_ssl_handshake("api.telegram.org", 443)
        .expect("SSL connect/handshake with Telegram API");
    println!("Created SSL stream");
    write!(
        &mut connection,
        "GET / HTTP/1.0\r\nHost: api.telegram.org\r\n\r\n"
    )
    .expect("write to the SSL stream");
    println!("Wrote request");
    let mut received = String::new();
    connection.read_to_string(&mut received).expect("Receive");
    println!("Received: {}", &received);
}
