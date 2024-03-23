use std::fs::File;
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
        Ok(_) => {}
        Err(error) => {
            println!("Could not write request to the API: {}.", error);
            return Err(error);
        }
    }
    Ok(())
}
