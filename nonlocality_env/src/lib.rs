#[deny(warnings)]
use std::fs::File;
#[cfg(any(unix, target_os = "wasi"))]
use std::os::fd::FromRawFd;

extern "C" {
    pub fn nonlocality_connect(interface: i32) -> i32;
    pub fn nonlocality_accept() -> u64;
    pub fn nonlocality_abort();
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

#[cfg(target_os = "windows")]
pub fn accept() -> Accepted {
    todo!();
}

#[cfg(any(unix, target_os = "wasi"))]
pub fn connect(interface: i32) -> File {
    let file_descriptor = unsafe { nonlocality_connect(interface) };
    let stream = unsafe { File::from_raw_fd(file_descriptor) };
    stream
}

#[cfg(target_os = "windows")]
pub fn connect(_interface: i32) -> File {
    todo!();
}
