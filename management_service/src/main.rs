use anyhow::bail;
use display_bytes::display_bytes;
use essrpc::transports::BincodeTransport;
use essrpc::RPCError;
use essrpc::RPCServer;
use management_interface::Blob;
use management_interface::ClusterConfiguration;
use management_interface::ConfigurationError;
use management_interface::IncomingInterface;
use management_interface::IncomingInterfaceId;
use management_interface::ManagementInterface;
use management_interface::ManagementInterfaceRPCServer;
use management_interface::OutgoingInterfaceId;
use management_interface::Service;
use management_interface::ServiceId;
use management_interface::WasiProcess;
use os_pipe::{pipe, PipeReader, PipeWriter};
use promising_future::{future_promise, Promise};
use std::any::Any;
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::env;
use std::fmt;
use std::io::Read;
use std::io::Write;
use std::path::Path;
use std::process::ExitCode;
use std::sync::{Arc, Mutex};
use std::thread;
use wasi_common::file::{FileAccessMode, FileType};
use wasi_common::pipe::WritePipe;
use wasi_common::sync::WasiCtxBuilder;
use wasi_common::ErrorExt;
use wasi_common::{WasiCtx, WasiFile};
use wasmtime::Config;
use wasmtime::{Caller, Engine, Linker, Module, Store};
use wasmtime_wasi_threads::WasiThreadsCtx;

struct Logger {
    name: String,
}

impl std::io::Write for Logger {
    fn write(&mut self, buf: &[u8]) -> std::result::Result<usize, std::io::Error> {
        let displayed_part = match buf {
            [head @ .., b'\n'] => head,
            _ => buf,
        };
        println!("{}: {}", self.name, display_bytes(displayed_part));
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::result::Result<(), std::io::Error> {
        Ok(())
    }
}

struct InterServiceApiStream {
    writer: Mutex<PipeWriter>,
    reader: Mutex<PipeReader>,
}

#[wiggle::async_trait]
impl WasiFile for InterServiceApiStream {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn get_filetype(&self) -> Result<FileType, wasi_common::Error> {
        Ok(FileType::Unknown)
    }

    async fn write_vectored<'a>(
        &self,
        _bufs: &[std::io::IoSlice<'a>],
    ) -> Result<u64, wasi_common::Error> {
        let mut writer = match self.writer.lock() {
            Ok(result) => result,
            Err(error) => {
                println!("Could not lock the pipe writer: {}.", error);
                return Err(wasi_common::Error::not_supported());
            }
        };
        match writer.write_vectored(_bufs) {
            Ok(written) => {
                println!("Wrote {} bytes to the pipe.", written);
                Ok(written as u64)
            }
            Err(error) => {
                println!("Writing to pipe failed with {}", error);
                Err(wasi_common::Error::io())
            }
        }
    }

    async fn read_vectored<'a>(
        &self,
        _bufs: &mut [std::io::IoSliceMut<'a>],
    ) -> Result<u64, wasi_common::Error> {
        let mut reader = match self.reader.lock() {
            Ok(result) => result,
            Err(error) => {
                println!("Could not lock the pipe reader: {}.", error);
                return Err(wasi_common::Error::not_supported());
            }
        };
        match reader.read_vectored(_bufs) {
            Ok(read) => {
                println!("Read {} bytes from the pipe.", read);
                Ok(read as u64)
            }
            Err(error) => Err(wasi_common::Error::from(error)),
        }
    }
}

#[derive(Debug)]
enum InterServiceApiError {
    OnlyOneAcceptorSupportedAtTheMoment,
    UnknownInternalError,
    CouldNotCreatePipe,
}

impl fmt::Display for InterServiceApiError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{}",
            match self {
                InterServiceApiError::OnlyOneAcceptorSupportedAtTheMoment =>
                    "only one acceptor supported at the moment",
                InterServiceApiError::UnknownInternalError => "unknown internal error",
                InterServiceApiError::CouldNotCreatePipe => "could not create an OS pipe",
            }
        )
    }
}

fn create_pair_of_streams(
) -> std::result::Result<(InterServiceApiStream, InterServiceApiStream), InterServiceApiError> {
    let upload = match pipe() {
        Ok(result) => result,
        Err(error) => {
            println!("Creating an OS pipe failed with {}.", error);
            return Err(InterServiceApiError::CouldNotCreatePipe);
        }
    };
    let download = match pipe() {
        Ok(result) => result,
        Err(error) => {
            println!("Creating an OS pipe failed with {}.", error);
            return Err(InterServiceApiError::CouldNotCreatePipe);
        }
    };
    let server_side = InterServiceApiStream {
        writer: Mutex::new(download.1),
        reader: Mutex::new(upload.0),
    };
    let client_side = InterServiceApiStream {
        writer: Mutex::new(upload.1),
        reader: Mutex::new(download.0),
    };
    return Ok((server_side, client_side));
}

#[cfg(test)]
fn check_transfer(to: &mut dyn std::io::Read, from: &mut dyn std::io::Write) {
    let message = &[1, 2, 3, 4];
    from.write_all(&message[..]).unwrap();
    let mut received: [u8; 4] = [0; 4];
    to.read_exact(&mut received[..]).unwrap();
    assert_eq!(&message[..], &received[..]);
}

#[test]
fn test_create_pair_of_streams() {
    use std::ops::DerefMut;
    let pair = create_pair_of_streams().unwrap();

    {
        let mut read = pair.0.reader.lock().unwrap();
        let mut write = pair.1.writer.lock().unwrap();
        check_transfer(&mut read.deref_mut(), &mut write.deref_mut());
    }

    {
        let mut read = pair.1.reader.lock().unwrap();
        let mut write = pair.0.writer.lock().unwrap();
        check_transfer(&mut read.deref_mut(), &mut write.deref_mut());
    }
}

struct AcceptResult {
    interface: IncomingInterfaceId,
    stream: InterServiceApiStream,
}

enum HubQueue {
    Accepting(Option<Promise<AcceptResult>>),
    Connecting(VecDeque<(IncomingInterfaceId, Promise<InterServiceApiStream>)>),
}

struct InterServiceApiHub {
    queue: Mutex<std::collections::BTreeMap<ServiceId, HubQueue>>,
}

impl InterServiceApiHub {
    pub fn new() -> InterServiceApiHub {
        InterServiceApiHub {
            queue: Mutex::new(std::collections::BTreeMap::new()),
        }
    }

    pub fn accept(
        &self,
        accepting_service: ServiceId,
    ) -> std::result::Result<AcceptResult, InterServiceApiError> {
        let mut locked = self.queue.lock().unwrap();
        let queue = locked
            .entry(accepting_service)
            .or_insert_with(|| HubQueue::Connecting(VecDeque::new()));
        match *queue {
            HubQueue::Accepting(_) => {
                Err(InterServiceApiError::OnlyOneAcceptorSupportedAtTheMoment)
            }
            HubQueue::Connecting(ref mut waiting) => match waiting.pop_front() {
                Some(next_in_line) => {
                    let (server_side, client_side) = create_pair_of_streams()?;
                    next_in_line.1.set(client_side);
                    Ok(AcceptResult {
                        interface: next_in_line.0,
                        stream: server_side,
                    })
                }
                None => {
                    let (future, promise) = future_promise();
                    *queue = HubQueue::Accepting(Some(promise));
                    drop(locked);
                    match future.value() {
                        Some(accept_result) => Ok(accept_result),
                        None => Err(InterServiceApiError::UnknownInternalError),
                    }
                }
            },
        }
    }

    pub fn connect(
        &self,
        destination_service: ServiceId,
        interface: IncomingInterfaceId,
    ) -> std::result::Result<InterServiceApiStream, InterServiceApiError> {
        let mut locked = self.queue.lock().unwrap();
        let queue = locked
            .entry(destination_service)
            .or_insert_with(|| HubQueue::Connecting(VecDeque::new()));
        match *queue {
            HubQueue::Accepting(ref mut acceptor) => {
                let (server_side, client_side) = create_pair_of_streams()?;
                let acceptor2: Promise<AcceptResult> = match acceptor.take() {
                    Some(content) => content,
                    None => panic!(),
                };
                acceptor2.set(AcceptResult {
                    interface: interface,
                    stream: server_side,
                });
                Ok(client_side)
            }
            HubQueue::Connecting(ref mut waiting) => {
                let (future, promise) = future_promise();
                waiting.push_back((interface, promise));
                drop(locked);
                match future.value() {
                    Some(stream) => Ok(stream),
                    None => Err(InterServiceApiError::UnknownInternalError),
                }
            }
        }
    }
}

#[derive(Clone)]
struct InterServiceFuncContext {
    wasi: WasiCtx,
    wasi_threads: Option<Arc<WasiThreadsCtx<InterServiceFuncContext>>>,
    // Somehow it's impossible to reference local variables from wasmtime host functions, so we have to use reference counting for no real reason.
    api_hub: Arc<InterServiceApiHub>,
    this_service_id: ServiceId,
    outgoing_interfaces: Arc<std::collections::BTreeMap<OutgoingInterfaceId, IncomingInterface>>,
}

// Absolutely ridiculous hack necessary because it is impossible to return multiple values,
// or return things by reference parameter in wasmtime.
fn encode_i32_pair(first: i32, second: i32) -> u64 {
    (((first as u32) as u64) << 32) | ((second as u32) as u64)
}

#[test]
fn test_encode_i32_pair() {
    assert_eq!(0, encode_i32_pair(0, 0));
    assert_eq!(4294967296, encode_i32_pair(1, 0));
    assert_eq!(1, encode_i32_pair(0, 1));
    assert_eq!(9223372032559808512, encode_i32_pair(i32::MAX, 0));
    assert_eq!(9223372034707292159, encode_i32_pair(i32::MAX, i32::MAX));
    assert_eq!(2147483647, encode_i32_pair(0, i32::MAX));
    assert_eq!(9223372036854775808, encode_i32_pair(i32::MIN, 0));
    assert_eq!(9223372039002259456, encode_i32_pair(i32::MIN, i32::MIN));
    assert_eq!(2147483648, encode_i32_pair(0, i32::MIN));
    assert_eq!(9223372039002259455, encode_i32_pair(i32::MIN, i32::MAX));
    assert_eq!(9223372034707292160, encode_i32_pair(i32::MAX, i32::MIN));
}

fn run_wasi_process(
    engine: Engine,
    module: Module,
    logger: Logger,
    api_hub: Arc<InterServiceApiHub>,
    has_threads: bool,
    this_service_id: ServiceId,
    outgoing_interfaces: Arc<std::collections::BTreeMap<OutgoingInterfaceId, IncomingInterface>>,
) -> wasmtime::Result<()> {
    let mut linker = Linker::new(&engine);
    wasi_common::sync::add_to_linker(&mut linker, |s: &mut InterServiceFuncContext| &mut s.wasi)?;
    let wasi = WasiCtxBuilder::new().build();

    let stdout = WritePipe::new(logger);
    wasi.set_stdout(Box::new(stdout.clone()));

    println!("Defining nonlocality_accept.");
    linker
        .func_wrap(
            "env",
            "nonlocality_accept",
            |caller: Caller<'_, InterServiceFuncContext>| -> u64 {
                println!("nonlocality_accept was called.");
                let context = caller.data();
                let accept_result = match context.api_hub.accept(context.this_service_id) {
                    Ok(success) => success,
                    Err(error) => {
                        println!("nonlocality_accept failed with {}.", error);
                        return encode_i32_pair(i32::max_value(), i32::max_value());
                    }
                };
                let file_descriptor = context
                    .wasi
                    .push_file(Box::new(accept_result.stream), FileAccessMode::all())
                    .unwrap() as i32;
                println!("nonlocality_accept returns FD {}.", file_descriptor);
                encode_i32_pair(accept_result.interface.0, file_descriptor)
            },
        )
        .expect("Tried to define nonlocality_accept");

    println!("Defining nonlocality_connect.");
    linker
        .func_wrap(
            "env",
            "nonlocality_connect",
            |caller: Caller<'_, InterServiceFuncContext>, interface: i32| -> i32 {
                println!(
                    "nonlocality_connect was called for interface {}.",
                    interface
                );
                let context = caller.data();
                let connecting_interface = match context
                    .outgoing_interfaces
                    .get(&OutgoingInterfaceId(interface))
                {
                    Some(found) => found,
                    None => todo!(),
                };
                let stream = match context.api_hub.connect(
                    connecting_interface.destination_service,
                    connecting_interface.interface,
                ) {
                    Ok(stream) => stream,
                    Err(error) => {
                        println!("nonlocality_connect failed with {}.", error);
                        return i32::max_value();
                    }
                };
                let stream_fd = context
                    .wasi
                    .push_file(Box::new(stream), FileAccessMode::all())
                    .unwrap() as i32;
                println!("nonlocality_connect returns FD {}.", stream_fd);
                stream_fd
            },
        )
        .expect("Tried to define nonlocality_connect");

    let mut func_context_store = Store::new(
        &engine,
        InterServiceFuncContext {
            wasi: wasi,
            wasi_threads: None,
            api_hub: api_hub.clone(),
            this_service_id: this_service_id,
            outgoing_interfaces: outgoing_interfaces,
        },
    );

    if has_threads {
        println!("Threads are enabled.");
        wasmtime_wasi_threads::add_to_linker(
            &mut linker,
            &func_context_store,
            &module,
            |s: &mut InterServiceFuncContext| &mut s.wasi_threads.as_ref().unwrap(),
        )
        .expect("Tried to add threads to the linker");
        func_context_store.data_mut().wasi_threads = Some(Arc::new(
            WasiThreadsCtx::new(module.clone(), Arc::new(linker.clone()))
                .expect("Tried to create a context"),
        ));
    } else {
        println!("Threads are not enabled.");
    }

    println!("Setting up the main module or something.");
    linker
        .module(&mut func_context_store, "", &module)
        .expect("Tried to module the main module, whatever that means");

    println!("Calling main function.");
    let entry_point = linker
        .get_default(&mut func_context_store, "")
        .expect("Tried to find the main entry point of the application");
    let typed_entry_point = entry_point
        .typed::<(), ()>(&func_context_store)
        .expect("Tried to cast the main entry point function type");
    match typed_entry_point.call(&mut func_context_store, ()) {
        Ok(_) => {
            println!("Main function returned.");
            Ok(())
        }
        Err(error) => bail!("Service {:?} failed: {:?}", this_service_id, error),
    }
}

struct ManagementInterfaceImpl {
    request_shutdown: tokio::sync::mpsc::Sender<()>,
}

impl ManagementInterface for ManagementInterfaceImpl {
    fn shutdown(&self) -> Result<bool, RPCError> {
        println!("Shutdown requested.");
        let handle = tokio::runtime::Handle::current();
        match handle.block_on(self.request_shutdown.send(())) {
            Ok(_) => Ok(true),
            Err(error) => {
                println!("Requesting shutdown failed: {}.", error);
                Ok(false)
            }
        }
    }

    fn reconfigure(
        &self,
        configuration: ClusterConfiguration,
    ) -> Result<Option<ConfigurationError>, RPCError> {
        println!("Reconfigure: {:?}", &configuration);
        Ok(Some(ConfigurationError::NotImplemented))
    }
}

fn handle_external_requests(
    stream: tokio::net::TcpStream,
    request_shutdown: tokio::sync::mpsc::Sender<()>,
) {
    let sync_stream = tokio_util::io::SyncIoBridge::new(stream);
    let mut server = ManagementInterfaceRPCServer::new(
        ManagementInterfaceImpl { request_shutdown },
        BincodeTransport::new(sync_stream),
    );
    match server.serve() {
        Ok(_) => {}
        Err(error) => {
            println!("External request server failed with {}.", error);
        }
    }
}

fn run_services(configuration: &ClusterConfiguration) -> bool {
    let api_hub = Arc::new(InterServiceApiHub::new());
    let is_success = thread::scope(|s| {
        let mut threads = Vec::new();
        for service in &configuration.services {
            println!("Starting thread for service {:?}.", service.id);
            let api_hub_2 = api_hub.clone();
            let this_service_id = service.id;
            let interfaces = Arc::new(service.outgoing_interfaces.clone());
            let wasm_code = match &service.wasi.code {
                management_interface::Blob::Digest(_) => todo!(),
                management_interface::Blob::Direct(content) => content.clone(),
            };
            let handler = s.spawn(move || {
                let mut config = Config::new();
                config.wasm_threads(service.wasi.has_threads);
                config.debug_info(true);
                config.wasm_backtrace(true);
                config.wasm_backtrace_details(wasmtime::WasmBacktraceDetails::Enable);
                let engine = match Engine::new(&config) {
                    Ok(success) => success,
                    Err(error) => {
                        panic!("Could not create wasmtime engine: {}.", error)
                    }
                };
                let module = match Module::from_binary(&engine, &wasm_code[..]) {
                    Ok(module) => module,
                    Err(error) => {
                        println!(
                            "Could not load wasm for service {:?}, error: {:?}.",
                            service.id, error
                        );
                        return Err(error);
                    }
                };
                run_wasi_process(
                    engine,
                    module,
                    Logger {
                        name: format!("Service#{:?}", service.id),
                    },
                    api_hub_2,
                    service.wasi.has_threads,
                    this_service_id,
                    interfaces,
                )
            });
            threads.push(handler);
        }

        let mut is_success = true;
        for thread in threads {
            println!("Waiting for a thread to complete.");
            match thread.join().unwrap() {
                Ok(_) => {}
                Err(error) => {
                    println!("One process failed with error: {}.", error);
                    is_success = false;
                }
            }
        }

        println!("All threads completed.");
        is_success
    });
    is_success
}

#[test]
fn test_run_services_empty_cluster() {
    let cluster_configuration = ClusterConfiguration {
        services: Vec::new(),
    };
    let is_success = run_services(&cluster_configuration);
    assert!(is_success);
}

fn create_hello_world_wasi_program() -> Vec<u8> {
    const HELLO_WORLD_WAT: &str = r#"(module
            ;; Import the required fd_write WASI function which will write the given io vectors to stdout
            ;; The function signature for fd_write is:
            ;; (File Descriptor, *iovs, iovs_len, *nwritten) -> Returns 0 on success, nonzero on error
            (import "wasi_snapshot_preview1" "fd_write" (func $fd_write (param i32 i32 i32 i32) (result i32)))
            
            (memory 1)
            (export "memory" (memory 0))
            
            ;; Write 'hello world\n' to memory at an offset of 8 bytes
            ;; Note the trailing newline which is required for the text to appear
            (data (i32.const 8) "hello world\n")
            
            (func $main (export "_start")
                ;; Creating a new io vector within linear memory
                (i32.store (i32.const 0) (i32.const 8))  ;; iov.iov_base - This is a pointer to the start of the 'hello world\n' string
                (i32.store (i32.const 4) (i32.const 12))  ;; iov.iov_len - The length of the 'hello world\n' string
            
                (call $fd_write
                    (i32.const 1) ;; file_descriptor - 1 for stdout
                    (i32.const 0) ;; *iovs - The pointer to the iov array, which is stored at memory location 0
                    (i32.const 1) ;; iovs_len - We're printing 1 string stored in an iov - so one.
                    (i32.const 20) ;; nwritten - A place in memory to store the number of bytes written
                )
                drop ;; Discard the number of bytes written from the top of the stack
            )
            )"#;
    wat::parse_str(HELLO_WORLD_WAT).expect("Tried to compile WAT code")
}

#[test]
fn test_run_services_one_finite_service() {
    let hello_world = create_hello_world_wasi_program();
    let cluster_configuration = ClusterConfiguration {
        services: vec![Service {
            id: ServiceId(0),
            outgoing_interfaces: BTreeMap::new(),
            wasi: WasiProcess {
                code: Blob::Direct(hello_world),
                has_threads: false,
            },
        }],
    };
    let is_success = run_services(&cluster_configuration);
    assert!(is_success);
}

#[test]
fn test_run_services_web_assembly_type_error() {
    // TODO: add assertions
    const TYPE_ERROR_PROGRAM: &str = r#"(module
        (memory 1)
        (export "memory" (memory 0))
        
        (func $main (export "_start")
            ;; there is nothing there to drop:
            drop
        )
        )"#;
    let type_error_program = wat::parse_str(TYPE_ERROR_PROGRAM).expect("Tried to compile WAT code");
    let cluster_configuration = ClusterConfiguration {
        services: vec![Service {
            id: ServiceId(0),
            outgoing_interfaces: BTreeMap::new(),
            wasi: WasiProcess {
                code: Blob::Direct(type_error_program),
                has_threads: false,
            },
        }],
    };
    let is_success = run_services(&cluster_configuration);
    assert!(!is_success);
}

#[test]
fn test_run_services_web_assembly_infinite_recursion() {
    // TODO: add assertions
    const RUNTIME_ERROR_PROGRAM: &str = r#"(module
        (memory 1)
        (export "memory" (memory 0))

        (func $recurse_infinitely
            (call $recurse_infinitely))

        (func $main (export "_start")
            (call $recurse_infinitely)
        )
        )"#;
    let runtime_error_program =
        wat::parse_str(RUNTIME_ERROR_PROGRAM).expect("Tried to compile WAT code");
    let cluster_configuration = ClusterConfiguration {
        services: vec![Service {
            id: ServiceId(0),
            outgoing_interfaces: BTreeMap::new(),
            wasi: WasiProcess {
                code: Blob::Direct(runtime_error_program),
                has_threads: false,
            },
        }],
    };
    let is_success = run_services(&cluster_configuration);
    assert!(!is_success);
}

#[test]
fn test_run_services_many_finite_services() {
    let hello_world = create_hello_world_wasi_program();
    let mut services = Vec::new();
    for i in 0..50 {
        services.push(Service {
            id: ServiceId(i),
            outgoing_interfaces: BTreeMap::new(),
            wasi: WasiProcess {
                code: Blob::Direct(hello_world.clone()),
                has_threads: false,
            },
        });
    }
    let cluster_configuration = ClusterConfiguration { services: services };
    let is_success = run_services(&cluster_configuration);
    assert!(is_success);
}

#[test]
fn test_run_services_inter_service_connect_accept() {
    // TODO: add assertions
    const API_PROVIDER: &str = r#"(module
        (import "env" "nonlocality_accept" (func $nonlocality_accept (result i64)))
        
        (memory 1)
        (export "memory" (memory 0))
        
        (func $main (export "_start")
            (call $nonlocality_accept)
            drop
        )
        )"#;
    let api_provider = wat::parse_str(API_PROVIDER).expect("Tried to compile WAT code");
    const API_CONSUMER: &str = r#"(module
        (import "env" "nonlocality_connect" (func $nonlocality_connect (param i32) (result i32)))
        
        (memory 1)
        (export "memory" (memory 0))
        
        (func $main (export "_start")
            (call $nonlocality_connect
                (i32.const 0) ;; OutgoingInterfaceId
            )
            drop
        )
        )"#;
    let api_consumer = wat::parse_str(API_CONSUMER).expect("Tried to compile WAT code");
    let cluster_configuration = ClusterConfiguration {
        services: vec![
            Service {
                id: ServiceId(0),
                outgoing_interfaces: BTreeMap::from([(
                    OutgoingInterfaceId(0),
                    IncomingInterface::new(ServiceId(1), IncomingInterfaceId(0)),
                )]),
                wasi: WasiProcess {
                    code: Blob::Direct(api_consumer),
                    has_threads: false,
                },
            },
            Service {
                id: ServiceId(1),
                outgoing_interfaces: BTreeMap::new(),
                wasi: WasiProcess {
                    code: Blob::Direct(api_provider),
                    has_threads: false,
                },
            },
        ],
    };
    let is_success = run_services(&cluster_configuration);
    assert!(is_success);
}

#[test]
fn test_run_services_inter_service_write_read() {
    // TODO: add assertions
    const API_PROVIDER: &str = r#"(module
        (import "env" "nonlocality_accept" (func $nonlocality_accept (result i64)))
        (import "wasi_snapshot_preview1" "fd_read" (func $fd_read (param i32 i32 i32 i32) (result i32)))
        
        (memory 1)
        (export "memory" (memory 0))
        
        ;; Buffer for fd_read
        (global $read_iovec i32 (i32.const 8100))
        (global $fdread_ret i32 (i32.const 8112))
        (global $read_buf i32 (i32.const 8120))

        (func $main (export "_start")
            (local $accept_result i64)
            (local $api_fd i32)
            (local $errno i32)

            (local.set $accept_result
                (call $nonlocality_accept))

            (local.set $api_fd
                (i32.wrap_i64 (local.get $accept_result)))

            (i32.store (global.get $read_iovec) (global.get $read_buf))
            (i32.store (i32.add (global.get $read_iovec) (i32.const 4)) (i32.const 128))

            (local.set $errno
                (call $fd_read
                    (local.get $api_fd)
                    (global.get $read_iovec)
                    (i32.const 1)
                    (global.get $fdread_ret)))
        )
        )"#;
    let api_provider = wat::parse_str(API_PROVIDER).expect("Tried to compile WAT code");
    const API_CONSUMER: &str = r#"(module
        (import "env" "nonlocality_connect" (func $nonlocality_connect (param i32) (result i32)))
        ;; The function signature for fd_write is:
        ;; (File Descriptor, *iovs, iovs_len, *nwritten) -> Returns 0 on success, nonzero on error
        (import "wasi_snapshot_preview1" "fd_write" (func $fd_write (param i32 i32 i32 i32) (result i32)))
        
        (memory 1)
        (export "memory" (memory 0))
        
        ;; Write 'hello world\n' to memory at an offset of 8 bytes
        ;; Note the trailing newline which is required for the text to appear
        (data (i32.const 8) "hello world\n")
        
        (func $main (export "_start")
            (local $api_fd i32)
            (local.set $api_fd
                (call $nonlocality_connect
                    (i32.const 0) ;; OutgoingInterfaceId
                ))

            ;; Creating a new io vector within linear memory
            (i32.store (i32.const 0) (i32.const 8))  ;; iov.iov_base - This is a pointer to the start of the 'hello world\n' string
            (i32.store (i32.const 4) (i32.const 12))  ;; iov.iov_len - The length of the 'hello world\n' string
        
            (call $fd_write
                (local.get $api_fd) ;; file_descriptor
                (i32.const 0) ;; *iovs - The pointer to the iov array, which is stored at memory location 0
                (i32.const 1) ;; iovs_len - We're printing 1 string stored in an iov - so one.
                (i32.const 20) ;; nwritten - A place in memory to store the number of bytes written
            )
            drop ;; Discard the number of bytes written from the top of the stack
        )
        )"#;
    let api_consumer = wat::parse_str(API_CONSUMER).expect("Tried to compile WAT code");
    let cluster_configuration = ClusterConfiguration {
        services: vec![
            Service {
                id: ServiceId(0),
                outgoing_interfaces: BTreeMap::from([(
                    OutgoingInterfaceId(0),
                    IncomingInterface::new(ServiceId(1), IncomingInterfaceId(0)),
                )]),
                wasi: WasiProcess {
                    code: Blob::Direct(api_consumer),
                    has_threads: false,
                },
            },
            Service {
                id: ServiceId(1),
                outgoing_interfaces: BTreeMap::new(),
                wasi: WasiProcess {
                    code: Blob::Direct(api_provider),
                    has_threads: false,
                },
            },
        ],
    };
    let is_success = run_services(&cluster_configuration);
    assert!(is_success);
}

async fn run_api_server(external_port_listener: tokio::net::TcpListener) {
    let (request_shutdown, mut shutdown_requested) = tokio::sync::mpsc::channel::<()>(1);
    loop {
        tokio::select! {
            maybe_accepted = external_port_listener.accept() => match maybe_accepted{
                Ok(incoming_connection) => {
                    println!(
                        "Accepted external API connection from {}.",
                        incoming_connection.1
                    );
                    let request_shutdown_clone = request_shutdown.clone();
                    tokio::task::spawn_blocking(move || {
                        handle_external_requests(incoming_connection.0, request_shutdown_clone);
                    });
                }
                Err(error) => {
                    println!("Accept failed with {}.", error);
                    break;
                }
            },
            maybe_received = shutdown_requested.recv() => {
                match maybe_received {
                    Some(_) => {
                        println!("Not accepting external connections anymore.");
                        break;
                    },
                    None => unreachable!("The sender remains on the stack, so the channel will never be closed."),
                }
            }
        }
    }
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> ExitCode {
    let args: Vec<String> = env::args().collect();

    let external_port = "127.0.0.1:6969";
    let external_port_listener = match tokio::net::TcpListener::bind(external_port).await {
        Ok(success) => success,
        Err(error) => {
            println!("Could not bind {}: {}", external_port, error);
            return ExitCode::FAILURE;
        }
    };

    let cluster_configuration_file_path = Path::new(&args[1]);
    println!(
        "Loading configuration from {}",
        cluster_configuration_file_path.display()
    );
    let cluster_configuration_content = tokio::fs::read(&cluster_configuration_file_path)
        .await
        .unwrap();
    let cluster_configuration = postcard::from_bytes(&cluster_configuration_content[..]).unwrap();
    let background_acceptor = tokio::spawn(run_api_server(external_port_listener));
    let is_success = run_services(&cluster_configuration);
    background_acceptor.await.unwrap();
    if is_success {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}
