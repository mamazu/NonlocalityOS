use display_bytes::display_bytes;
use std::fs::File;
use std::io::Read;
use std::io::Write;
#[cfg(any(unix, target_os = "wasi"))]
use std::os::fd::{FromRawFd, IntoRawFd, RawFd};

extern "C" {
    fn connect() -> i32;
}

#[cfg(any(unix, target_os = "wasi"))]
fn main() -> Result<(), std::io::Error> {
    println!("Connecting to an API..");
    let api_fd = unsafe { connect() };
    let mut file = unsafe { File::from_raw_fd(api_fd) };
    match write!(&mut file, "my request") {
        Ok(_) => {
            println!("Wrote request to the API.");
        }
        Err(error) => {
            println!("Could not write request to the API: {}.", error);
            return Err(error);
        }
    }
    let mut read_buffer = [0; 17];
    match file.read_exact(&mut read_buffer) {
        Ok(_) => {
            println!(
                "Read response from the API: {}.",
                display_bytes(&read_buffer)
            );
        }
        Err(error) => {
            println!("Could not read response from the API: {}.", error);
            return Err(error);
        }
    }
    Ok(())
}
