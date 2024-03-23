use display_bytes::display_bytes;
use std::fs::File;
use std::io::Read;
use std::io::Write;
#[cfg(any(unix, target_os = "wasi"))]
use std::os::fd::{FromRawFd, IntoRawFd, RawFd};

extern "C" {
    fn nonlocality_accept() -> i32;
}

#[cfg(any(unix, target_os = "wasi"))]
fn main() -> Result<(), std::io::Error> {
    println!("Accepting an API client..");
    let api_fd = unsafe { nonlocality_accept() };
    println!("Accepted an API client..");
    let mut file = unsafe { File::from_raw_fd(api_fd) };

    let mut read_buffer = [0; 10];
    let request = match file.read_exact(&mut read_buffer) {
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
    match write!(&mut file, "{}", response) {
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
