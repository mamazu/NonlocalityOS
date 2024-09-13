#[cfg(any(unix, target_os = "wasi"))]
fn main() -> Result<(), std::io::Error> {
    use std::fs::File;
    use std::io::Read;
    use std::io::Write;
    use std::os::fd::FromRawFd;
    println!("Connecting to an API server..");
    let api_fd = unsafe { nonlocality_env::nonlocality_connect(0) };
    println!("Connected to an API server..");
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
            let response = std::str::from_utf8(&read_buffer).unwrap();
            println!("Read response from the API: {}.", response);
            assert!(response == "response: success");
        }
        Err(error) => {
            println!("Could not read response from the API: {}.", error);
            return Err(error);
        }
    }

    Ok(())
}
