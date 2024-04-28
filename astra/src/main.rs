#![deny(warnings)]
use async_recursion::async_recursion;
use std::collections::BTreeMap;
pub mod downloads;

#[derive(Clone)]
struct Program {}

#[derive(Clone)]
enum DirectoryEntry {
    Program(Program),
    Directory(Directory),
}

#[derive(Clone)]
struct Directory {
    entries: BTreeMap<String, DirectoryEntry>,
}

#[derive(Debug, PartialEq, Clone, Copy)]
struct NumberOfErrors(u64);

impl std::ops::Add<NumberOfErrors> for NumberOfErrors {
    type Output = NumberOfErrors;

    fn add(self, rhs: NumberOfErrors) -> NumberOfErrors {
        let (sum, has_overflown) = u64::overflowing_add(self.0, rhs.0);
        assert!(!has_overflown);
        NumberOfErrors(sum)
    }
}

impl std::ops::AddAssign for NumberOfErrors {
    fn add_assign(&mut self, rhs: NumberOfErrors) {
        *self = *self + rhs;
    }
}

async fn run_process_with_error_only_output(
    working_directory: &std::path::Path,
    executable: &std::path::Path,
    arguments: &[&str],
) -> NumberOfErrors {
    let maybe_output = tokio::process::Command::new(executable)
        .args(arguments)
        .current_dir(&working_directory)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .output()
        .await;
    let output = match maybe_output {
        Ok(output) => output,
        Err(error) => {
            println!(
                "Failed to spawn {} process: {}.",
                executable.display(),
                error
            );
            return NumberOfErrors(1);
        }
    };
    if output.status.success() {
        return NumberOfErrors(0);
    }
    println!("Executable: {}", executable.display());
    println!("Working directory: {}", working_directory.display());
    println!("Exit status: {}", output.status);
    println!("Standard output:");
    let stdout = String::from_utf8_lossy(&output.stdout);
    println!("{}", &stdout);
    println!("Standard error:");
    let stderr = String::from_utf8_lossy(&output.stderr);
    println!("{}", &stderr);
    NumberOfErrors(1)
}

async fn run_cargo(project: &std::path::Path, arguments: &[&str]) -> NumberOfErrors {
    run_process_with_error_only_output(&project, std::path::Path::new("cargo"), &arguments).await
}

async fn run_cargo_fmt(project: &std::path::Path) -> NumberOfErrors {
    run_cargo(&project, &["fmt"]).await
}

async fn run_cargo_test(project: &std::path::Path) -> NumberOfErrors {
    run_cargo(&project, &["test"]).await
}

async fn build_and_test_program(
    _program: &Program,
    where_in_filesystem: &std::path::Path,
) -> NumberOfErrors {
    run_cargo_fmt(&where_in_filesystem).await + run_cargo_test(&where_in_filesystem).await
}

#[async_recursion]
async fn build_and_test_directory_entry(
    directory_entry: &DirectoryEntry,
    where_in_filesystem: &std::path::Path,
) -> NumberOfErrors {
    let mut error_count = NumberOfErrors(0);
    match directory_entry {
        DirectoryEntry::Program(program) => {
            error_count += build_and_test_program(&program, &where_in_filesystem).await;
        }
        DirectoryEntry::Directory(directory) => {
            error_count += build_and_test_recursively(&directory, &where_in_filesystem).await;
        }
    }
    error_count
}

#[async_recursion]
async fn build_and_test_recursively(
    description: &Directory,
    where_in_filesystem: &std::path::Path,
) -> NumberOfErrors {
    let mut error_count = NumberOfErrors(0);
    for entry in &description.entries {
        let subdirectory = where_in_filesystem.join(entry.0);
        error_count += build_and_test_directory_entry(&entry.1, &subdirectory).await;
    }
    error_count
}

async fn install_raspberry_pi_cpp_compiler(tools_directory: &std::path::Path) -> NumberOfErrors {
    // found this compiler on https://developer.arm.com/downloads/-/gnu-a
    let compiler_name = "gcc-arm-10.3-2021.07-mingw-w64-i686-aarch64-none-linux-gnu";
    let archive_file_name = format!("{}.tar.xz", compiler_name);
    let download_url = format!("https://developer.arm.com/-/media/Files/downloads/gnu-a/10.3-2021.07/binrel/{}?rev=06b6c36e428c48fda4b6d907f17308be^&hash=B36CC5C9544DCFCB2DB06FB46C8B8262", &archive_file_name);
    let unpacked_directory = tools_directory.join("raspberry_pi_compiler");
    match downloads::install_from_downloaded_archive(
        &download_url,
        &tools_directory.join(&archive_file_name),
        &unpacked_directory,
        downloads::Compression::Xz,
    ) {
        Ok(_) => NumberOfErrors(0),
        Err(error) => {
            println!("Could not download and unpack {}: {}", &download_url, error);
            NumberOfErrors(1)
        }
    }
}

async fn install_wasi_cpp_compiler(tools_directory: &std::path::Path) -> NumberOfErrors {
    let compiler_name = "wasi-sdk-22";
    let archive_file_name = format!("{}.0.m-mingw.tar.gz", compiler_name);
    let download_url = format!(
        "https://github.com/WebAssembly/wasi-sdk/releases/download/{}/{}",
        &compiler_name, &archive_file_name
    );
    let unpacked_directory = tools_directory.join(format!("{}.0.m-mingw", compiler_name));
    match downloads::install_from_downloaded_archive(
        &download_url,
        &tools_directory.join(&archive_file_name),
        &unpacked_directory,
        downloads::Compression::Gz,
    ) {
        Ok(_) => NumberOfErrors(0),
        Err(error) => {
            println!("Could not download and unpack {}: {}", &download_url, error);
            NumberOfErrors(1)
        }
    }
}

async fn install_rust_toolchain(
    working_directory: &std::path::Path,
    target_name: &str,
    channel: &str,
) -> NumberOfErrors {
    run_process_with_error_only_output(
        working_directory,
        std::path::Path::new("rustup"),
        &["target", "add", &target_name, "--toolchain", &channel],
    )
    .await
}

async fn install_rust_toolchains(working_directory: &std::path::Path) -> NumberOfErrors {
    install_rust_toolchain(working_directory, "aarch64-unknown-linux-gnu", "stable").await
        + install_rust_toolchain(working_directory, "wasm32-wasi", "stable").await
        + install_rust_toolchain(working_directory, "wasm32-wasip1-threads", "nightly").await
}

async fn install_tools(repository: &std::path::Path) -> NumberOfErrors {
    let tools_directory = repository.join("tools");
    install_raspberry_pi_cpp_compiler(&tools_directory).await
        + install_wasi_cpp_compiler(&tools_directory).await
        + install_rust_toolchains(&repository).await
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> std::process::ExitCode {
    let command_line_arguments: Vec<String> = std::env::args().collect();
    if command_line_arguments.len() != 2 {
        println!("One command line argument required: Path to the root of the repository.");
        return std::process::ExitCode::FAILURE;
    }
    let repository = std::path::Path::new(&command_line_arguments[1]);

    let root = Directory {
        entries: BTreeMap::from([
            ("astra".to_string(), DirectoryEntry::Program(Program {})),
            (
                "example_applications".to_string(),
                DirectoryEntry::Directory(Directory {
                    entries: BTreeMap::from([(
                        "rust".to_string(),
                        DirectoryEntry::Directory(Directory {
                            entries: BTreeMap::from([
                                ("call_api".to_string(), DirectoryEntry::Program(Program {})),
                                (
                                    "database".to_string(),
                                    DirectoryEntry::Directory(Directory {
                                        entries: BTreeMap::from([
                                            (
                                                "database_client".to_string(),
                                                DirectoryEntry::Program(Program {}),
                                            ),
                                            (
                                                "database_server".to_string(),
                                                DirectoryEntry::Program(Program {}),
                                            ),
                                            (
                                                "database_trait".to_string(),
                                                DirectoryEntry::Program(Program {}),
                                            ),
                                        ]),
                                    }),
                                ),
                                (
                                    "essrpc_example".to_string(),
                                    DirectoryEntry::Directory(Directory {
                                        entries: BTreeMap::from([
                                            (
                                                "essrpc_client".to_string(),
                                                DirectoryEntry::Program(Program {}),
                                            ),
                                            (
                                                "essrpc_server".to_string(),
                                                DirectoryEntry::Program(Program {}),
                                            ),
                                            (
                                                "essrpc_trait".to_string(),
                                                DirectoryEntry::Program(Program {}),
                                            ),
                                        ]),
                                    }),
                                ),
                                (
                                    "hello_rust".to_string(),
                                    DirectoryEntry::Program(Program {}),
                                ),
                                (
                                    "idle_service".to_string(),
                                    DirectoryEntry::Program(Program {}),
                                ),
                                (
                                    "provide_api".to_string(),
                                    DirectoryEntry::Program(Program {}),
                                ),
                            ]),
                        }),
                    )]),
                }),
            ),
            (
                "management_interface".to_string(),
                DirectoryEntry::Program(Program {}),
            ),
            (
                "management_service".to_string(),
                DirectoryEntry::Program(Program {}),
            ),
            (
                "nonlocality_env".to_string(),
                DirectoryEntry::Program(Program {}),
            ),
        ]),
    };

    let mut error_count = install_tools(repository).await;
    error_count += build_and_test_recursively(&root, &repository).await;
    match error_count.0 {
        0 => std::process::ExitCode::SUCCESS,
        _ => {
            println!("{} errors.", error_count.0);
            std::process::ExitCode::FAILURE
        }
    }
}
