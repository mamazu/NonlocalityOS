use display_bytes::display_bytes;
use normalize_path::NormalizePath;
use relative_path::RelativePathBuf;
use std::any::Any;
use std::env;
use std::io::Read;
use std::path::Path;
use std::process::ExitCode;
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

struct InterServiceApiStream {}

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
        let mut total_size: u64 = 0;
        for buffer in _bufs {
            total_size += buffer.len() as u64;
        }
        println!("Placeholder API received {} bytes.", total_size);
        Ok(total_size)
    }

    async fn read_vectored<'a>(
        &self,
        _bufs: &mut [std::io::IoSliceMut<'a>],
    ) -> Result<u64, wasi_common::Error> {
        let mut data: &[u8] = b"response: success";
        match data.read_vectored(_bufs) {
            Ok(bytes_read) => Ok(bytes_read as u64),
            Err(error) => {
                println!(
                    "Reading the response failed (should not happen): {}.",
                    error
                );
                Err(wasi_common::Error::not_supported())
            }
        }
    }
}

fn run_wasi_process(engine: Engine, module: Module, logger: Logger) -> wasmtime::Result<()> {
    let mut linker = Linker::new(&engine);
    wasi_common::sync::add_to_linker(&mut linker, |s| s)?;
    // TODO: use WasiCtx::new
    let wasi = WasiCtxBuilder::new().build();

    let stdout = WritePipe::new(logger);
    wasi.set_stdout(Box::new(stdout.clone()));

    let mut store_wasi = Store::new(&engine, wasi);

    linker.func_wrap(
        "env",
        "nonlocality_accept",
        |caller: Caller<'_, WasiCtx>| {
            println!("nonlocality_accept was called.");
            let stream = InterServiceApiStream {};
            let stream_fd = caller
                .data()
                .push_file(Box::new(stream), FileAccessMode::all())
                .unwrap();
            println!("nonlocality_accept returns FD {}.", stream_fd);
            stream_fd
        },
    )?;

    linker.func_wrap(
        "env",
        "nonlocality_connect",
        |caller: Caller<'_, WasiCtx>| {
            println!("nonlocality_connect was called.");
            let stream = InterServiceApiStream {};
            let stream_fd = caller
                .data()
                .push_file(Box::new(stream), FileAccessMode::all())
                .unwrap();
            println!("nonlocality_connect returns FD {}.", stream_fd);
            stream_fd
        },
    )?;

    linker.module(&mut store_wasi, "", &module)?;
    linker
        .get_default(&mut store_wasi, "")?
        .typed::<(), ()>(&store_wasi)?
        .call(&mut store_wasi, ())?;
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
                panic!("TO DO");
            }
        };
        println!("Starting thread for {}.", input_program_path.display());
        let handler = thread::spawn(move || {
            run_wasi_process(
                engine,
                module,
                Logger {
                    name: input_program_path.display().to_string(),
                },
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
}
