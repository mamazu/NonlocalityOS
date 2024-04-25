use std::fs::File;
#[cfg(any(unix, target_os = "wasi"))]
use std::os::fd::{FromRawFd, IntoRawFd, RawFd};

extern "C" {
    pub fn nonlocality_connect(interface: i32) -> i32;
    pub fn nonlocality_accept() -> u64;
}

pub struct Accepted {
    pub interface: i32,
    pub stream: File,
}

#[cfg(any(unix, target_os = "wasi"))]
pub fn accept() -> Accepted {
    let encoded_result = unsafe { nonlocality_accept() };
    let interface = (encoded_result >> 32) as i32;
    let stream = unsafe { File::from_raw_fd((encoded_result & (u32::max_value() as u64)) as i32) };
    Accepted { interface, stream }
}
