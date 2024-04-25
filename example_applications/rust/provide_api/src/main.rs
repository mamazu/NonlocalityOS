use display_bytes::display_bytes;
use nonlocality_env::accept;
use std::fs::File;
use std::io::Read;
use std::io::Write;

fn main() -> Result<(), std::io::Error> {
    println!("Accepting an API client..");
    let mut accepted = accept();
    println!(
        "Accepted an API client for interface{}.",
        accepted.interface
    );

    let mut read_buffer = [0; 10];
    let request = match accepted.stream.read_exact(&mut read_buffer) {
        Ok(_) => {
            let request = std::str::from_utf8(&read_buffer).unwrap();
            println!("Read request: {}.", request);
            request
        }
        Err(error) => {
            println!("Could not read request: {}.", error);
            return Err(error);
        }
    };

    let response = if (request == "my request") {
        "response: success"
    } else {
        "unknown request!!"
    };
    match write!(&mut accepted.stream, "{}", response) {
        Ok(_) => {
            println!("Wrote response.");
        }
        Err(error) => {
            println!("Could not write response: {}.", error);
            return Err(error);
        }
    }

    Ok(())
}
