use display_bytes::display_bytes;
use normalize_path::NormalizePath;
use os_pipe::{pipe, PipeReader, PipeWriter};
use promising_future::{future_promise, Promise};
use relative_path::RelativePathBuf;
use std::any::Any;
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
use wasmtime::{Caller, Engine, Linker, Module, Store};

struct WasiProcess {
    web_assembly_file: RelativePathBuf,
}

struct Order {
    wasi_processes: Vec<WasiProcess>,
}

struct Logger {
    name: String,
}

impl std::io::Write for Logger {
    fn write(&mut self, buf: &[u8]) -> std::result::Result<usize, std::io::Error> {
        println!("{}: {}", self.name, display_bytes(buf));
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
            Err(error) => Err(wasi_common::Error::from(error)),
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
struct InterServiceApiHub {
    acceptor: Mutex<Option<Promise<InterServiceApiStream>>>,
}

impl InterServiceApiHub {
    pub fn new() -> InterServiceApiHub {
        InterServiceApiHub {
            acceptor: Mutex::new(None),
        }
    }

    pub fn accept(&self) -> std::result::Result<InterServiceApiStream, InterServiceApiError> {
        let (future, promise) = future_promise();
        {
            let mut maybe_acceptor = self.acceptor.lock().unwrap();
            match *maybe_acceptor {
                Some(_) => return Err(InterServiceApiError::OnlyOneAcceptorSupportedAtTheMoment),
                None => *maybe_acceptor = Some(promise),
            }
        }
        match future.value() {
            Some(stream) => Ok(stream),
            None => Err(InterServiceApiError::UnknownInternalError),
        }
    }

    pub fn connect(&self) -> std::result::Result<InterServiceApiStream, InterServiceApiError> {
        let mut maybe_acceptor = self.acceptor.lock().unwrap();
        match maybe_acceptor.take() {
            Some(acceptor) => {
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
                acceptor.set(server_side);
                let client_side = InterServiceApiStream {
                    writer: Mutex::new(upload.1),
                    reader: Mutex::new(download.0),
                };
                Ok(client_side)
            }
            None => todo!(),
        }
    }
}

struct InterServiceFuncContext {
    wasi: WasiCtx,
    // Somehow it's impossible to reference local variables from wasmtime host functions, so we have to use reference counting for no real reason.
    api_hub: Arc<InterServiceApiHub>,
}

fn run_wasi_process(
    engine: Engine,
    module: Module,
    logger: Logger,
    api_hub: Arc<InterServiceApiHub>,
) -> wasmtime::Result<()> {
    let mut linker = Linker::new(&engine);
    wasi_common::sync::add_to_linker(&mut linker, |s: &mut InterServiceFuncContext| &mut s.wasi)?;
    // TODO: use WasiCtx::new
    let wasi = WasiCtxBuilder::new().build();

    let stdout = WritePipe::new(logger);
    wasi.set_stdout(Box::new(stdout.clone()));

    let mut func_context_store = Store::new(
        &engine,
        InterServiceFuncContext {
            wasi: wasi,
            api_hub: api_hub.clone(),
        },
    );

    linker.func_wrap(
        "env",
        "nonlocality_accept",
        |caller: Caller<'_, InterServiceFuncContext>| {
            println!("nonlocality_accept was called.");
            let context = caller.data();
            let stream = match context.api_hub.accept() {
                Ok(stream) => stream,
                Err(error) => {
                    println!("nonlocality_accept failed with {}.", error);
                    return u32::max_value();
                }
            };
            let stream_fd = context
                .wasi
                .push_file(Box::new(stream), FileAccessMode::all())
                .unwrap();
            println!("nonlocality_accept returns FD {}.", stream_fd);
            stream_fd
        },
    )?;

    linker.func_wrap(
        "env",
        "nonlocality_connect",
        |caller: Caller<'_, InterServiceFuncContext>| {
            println!("nonlocality_connect was called.");
            let context = caller.data();
            let stream = match context.api_hub.connect() {
                Ok(stream) => stream,
                Err(error) => {
                    println!("nonlocality_connect failed with {}.", error);
                    return u32::max_value();
                }
            };
            let stream_fd = context
                .wasi
                .push_file(Box::new(stream), FileAccessMode::all())
                .unwrap();
            println!("nonlocality_connect returns FD {}.", stream_fd);
            stream_fd
        },
    )?;

    linker.module(&mut func_context_store, "", &module)?;
    linker
        .get_default(&mut func_context_store, "")?
        .typed::<(), ()>(&func_context_store)?
        .call(&mut func_context_store, ())?;
    Ok(())
}

fn main() -> ExitCode {
    let args: Vec<String> = env::args().collect();
    let repository = Path::new(&args[1]).normalize();
    let order = Order {
        wasi_processes: vec![
            WasiProcess {
                web_assembly_file: RelativePathBuf::from_path(
                    "example_applications/rust/hello_rust/target/wasm32-wasi/debug/hello_rust.wasm",
                )
                .unwrap(),
            },
            WasiProcess {
                web_assembly_file: RelativePathBuf::from_path(
                    "example_applications/rust/provide_api/target/wasm32-wasi/debug/provide_api.wasm",
                )
                .unwrap(),
            },
            WasiProcess {
                web_assembly_file: RelativePathBuf::from_path(
                    "example_applications/rust/call_api/target/wasm32-wasi/debug/call_api.wasm",
                )
                .unwrap(),
            },
            /*WasiProcess {
                web_assembly_file: RelativePathBuf::from_path(
                    "example_applications/rust/idle_service/target/wasm32-wasi/debug/idle_service.wasm",
                )
                .unwrap(),
            }*/
        ],
    };

    let api_hub = Arc::new(InterServiceApiHub::new());
    thread::scope(|s| {
        let mut threads = Vec::new();
        for wasi_process in order.wasi_processes {
            let engine = Engine::default();
            let input_program_path = wasi_process.web_assembly_file.to_path(&repository);
            let module = match Module::from_file(&engine, &input_program_path) {
                Ok(module) => module,
                Err(error) => {
                    println!(
                        "Could not load {}, error: {}.",
                        input_program_path.display(),
                        error
                    );
                    todo!()
                }
            };
            println!("Starting thread for {}.", input_program_path.display());
            let api_hub_2 = api_hub.clone();
            let handler = s.spawn(move || {
                run_wasi_process(
                    engine,
                    module,
                    Logger {
                        name: input_program_path.display().to_string(),
                    },
                    api_hub_2,
                )
            });
            threads.push(handler);
        }

        let mut exit_code = ExitCode::SUCCESS;
        for thread in threads {
            println!("Waiting for a thread to complete.");
            match thread.join().unwrap() {
                Ok(_) => {}
                Err(error) => {
                    println!("One process failed with error: {}.", error);
                    exit_code = ExitCode::FAILURE;
                }
            }
        }
        println!("All threads completed.");
        exit_code
    })
}
