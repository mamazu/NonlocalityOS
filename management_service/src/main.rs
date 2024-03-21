use normalize_path::NormalizePath;
use relative_path::RelativePathBuf;
use std::env;
use std::path::Path;
use std::thread;
use wasi_common::sync::WasiCtxBuilder;
use wasmtime::*;

struct WasiProcess {
    web_assembly_file: RelativePathBuf,
}

struct Order {
    wasi_processes: Vec<WasiProcess>,
}

fn run_wasi_process(engine: Engine, module: Module) -> wasmtime::Result<()> {
    let mut linker = Linker::new(&engine);
    wasi_common::sync::add_to_linker(&mut linker, |s| s)?;
    // TODO: use WasiCtx::new
    let wasi = WasiCtxBuilder::new().build();
    let mut store = Store::new(&engine, wasi);
    linker.module(&mut store, "", &module)?;
    linker
        .get_default(&mut store, "")?
        .typed::<(), ()>(&store)?
        .call(&mut store, ())?;
    Ok(())
}

fn main() -> Result<(), std::io::Error> {
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
                    "example_applications/rust/idle_service/target/wasm32-wasi/debug/idle_service.wasm",
                )
                .unwrap(),
            }
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
        let handler = thread::spawn(|| run_wasi_process(engine, module));
        threads.push(handler);
    }
    for thread in threads {
        println!("Waiting for a thread to complete.");
        match thread.join().unwrap() {
            Ok(_) => {}
            Err(error) => {
                println!("One process failed with error: {}.", error);
                panic!("TO DO");
            }
        }
    }
    println!("All threads completed.");
    Ok(())
}
