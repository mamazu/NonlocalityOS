#![deny(warnings)]
use async_recursion::async_recursion;
use std::collections::{BTreeMap, HashMap};
pub mod downloads;

#[derive(Clone)]
struct RaspberryPi64Target {
    compiler_installation: std::path::PathBuf,
}

#[derive(Clone)]
enum CargoBuildTarget {
    Host,
    RaspberryPi64(RaspberryPi64Target),
}

#[derive(Clone)]
struct Program {
    pub targets: Vec<CargoBuildTarget>,
}

impl Program {
    pub fn host() -> Program {
        Program {
            targets: vec![CargoBuildTarget::Host],
        }
    }

    pub fn host_and_pi(pi: RaspberryPi64Target) -> Program {
        Program {
            targets: vec![CargoBuildTarget::Host, CargoBuildTarget::RaspberryPi64(pi)],
        }
    }

    pub fn other() -> Program {
        Program { targets: vec![] }
    }
}

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
    environment_variables: &HashMap<String, String>,
) -> NumberOfErrors {
    let maybe_output = tokio::process::Command::new(executable)
        .args(arguments)
        .current_dir(&working_directory)
        .envs(environment_variables)
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
    println!("Arguments: {}", arguments.join(" "));
    println!("Environment: {:?}", &environment_variables);
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

async fn run_cargo(
    project: &std::path::Path,
    arguments: &[&str],
    environment_variables: &HashMap<String, String>,
) -> NumberOfErrors {
    run_process_with_error_only_output(
        &project,
        std::path::Path::new("cargo"),
        &arguments,
        &environment_variables,
    )
    .await
}

async fn run_cargo_fmt(project: &std::path::Path) -> NumberOfErrors {
    run_cargo(&project, &["fmt"], &HashMap::new()).await
}

async fn run_cargo_test(project: &std::path::Path) -> NumberOfErrors {
    run_cargo(&project, &["test"], &HashMap::new()).await
}

const RASPBERRY_PI_TARGET_NAME: &str = "aarch64-unknown-linux-gnu";

fn confirm_regular_file(path: &std::path::Path) -> bool {
    match std::fs::metadata(path) {
        Ok(info) => {
            if info.is_file() {
                true
            } else {
                println!(
                    "Expected file at {}, but found something else.",
                    path.display()
                );
                false
            }
        }
        Err(error) => {
            println!(
                "Expected file at {}, but got an error: {}",
                path.display(),
                error
            );
            false
        }
    }
}

fn confirm_directory(path: &std::path::Path) -> bool {
    match std::fs::metadata(path) {
        Ok(info) => {
            if info.is_dir() {
                true
            } else {
                println!(
                    "Expected directory at {}, but found something else.",
                    path.display()
                );
                false
            }
        }
        Err(error) => {
            println!(
                "Expected directory at {}, but got an error: {}",
                path.display(),
                error
            );
            false
        }
    }
}

async fn run_cargo_build_for_raspberry_pi(
    project: &std::path::Path,
    compiler_installation: &std::path::Path,
) -> NumberOfErrors {
    let target_name = RASPBERRY_PI_TARGET_NAME;
    let bin = compiler_installation.join("bin");
    let ar = bin.join("aarch64-none-linux-gnu-ar.exe");
    if !confirm_regular_file(&ar) {
        return NumberOfErrors(1);
    }
    let ar_str = ar.to_str().expect("Tried to convert path to string");
    let compiler = bin.join("aarch64-none-linux-gnu-gcc.exe");
    if !confirm_regular_file(&compiler) {
        return NumberOfErrors(1);
    }
    let compiler_str = compiler.to_str().expect("Tried to convert path to string");
    let library_path = compiler_installation
        .join("aarch64-none-linux-gnu")
        .join("libc")
        .join("lib64");
    if !confirm_directory(&library_path) {
        return NumberOfErrors(1);
    }
    let library_path_str = library_path
        .to_str()
        .expect("Tried to convert path to string");
    let environment_variables = HashMap::from([
        (format!("CC_{}", target_name), compiler_str.to_string()),
        (format!("AR_{}", target_name), ar_str.to_string()),
        ("LD_LIBRARY_PATH".to_string(), library_path_str.to_string()),
    ]);
    run_cargo(
        &project,
        &[
            "build",
            "--target",
            target_name,
            "--config",
            &format!("target.{}.linker='{}'", target_name, compiler_str),
            "--release",
        ],
        &environment_variables,
    )
    .await
}

async fn run_cargo_build(project: &std::path::Path, target: &CargoBuildTarget) -> NumberOfErrors {
    match target {
        CargoBuildTarget::Host => run_cargo(&project, &["build"], &HashMap::new()).await,
        CargoBuildTarget::RaspberryPi64(pi) => {
            run_cargo_build_for_raspberry_pi(&project, &pi.compiler_installation).await
        }
    }
}

async fn build_relevant_targets(
    program: &Program,
    where_in_filesystem: &std::path::Path,
) -> NumberOfErrors {
    let mut error_count = NumberOfErrors(0);
    for target in &program.targets {
        error_count += run_cargo_build(&where_in_filesystem, &target).await
    }
    error_count
}

async fn build_and_test_program(
    program: &Program,
    where_in_filesystem: &std::path::Path,
) -> NumberOfErrors {
    run_cargo_fmt(&where_in_filesystem).await
        + run_cargo_test(&where_in_filesystem).await
        + build_relevant_targets(program, where_in_filesystem).await
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

async fn install_raspberry_pi_cpp_compiler(
    tools_directory: &std::path::Path,
) -> (NumberOfErrors, Option<RaspberryPi64Target>) {
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
        Ok(_) => (
            NumberOfErrors(0),
            Some(RaspberryPi64Target {
                compiler_installation: unpacked_directory.join(compiler_name),
            }),
        ),
        Err(error) => {
            println!("Could not download and unpack {}: {}", &download_url, error);
            (NumberOfErrors(1), None)
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
        &HashMap::new(),
    )
    .await
}

async fn install_rust_toolchains(working_directory: &std::path::Path) -> NumberOfErrors {
    install_rust_toolchain(working_directory, RASPBERRY_PI_TARGET_NAME, "stable").await
        + install_rust_toolchain(working_directory, "wasm32-wasi", "stable").await
        + install_rust_toolchain(working_directory, "wasm32-wasip1-threads", "nightly").await
}

async fn install_tools(
    repository: &std::path::Path,
) -> (NumberOfErrors, Option<RaspberryPi64Target>) {
    let tools_directory = repository.join("tools");
    let (error_count, raspberry_pi) = install_raspberry_pi_cpp_compiler(&tools_directory).await;
    (
        error_count
            + install_wasi_cpp_compiler(&tools_directory).await
            + install_rust_toolchains(&repository).await,
        raspberry_pi,
    )
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> std::process::ExitCode {
    let command_line_arguments: Vec<String> = std::env::args().collect();
    if command_line_arguments.len() != 2 {
        println!("One command line argument required: Path to the root of the repository.");
        return std::process::ExitCode::FAILURE;
    }
    let repository = std::path::Path::new(&command_line_arguments[1]);
    let (mut error_count, maybe_raspberry_pi) = install_tools(repository).await;

    let root = Directory {
        entries: BTreeMap::from([
            (
                "admin_tool".to_string(),
                DirectoryEntry::Program(Program::host()),
            ),
            (
                "astra".to_string(),
                DirectoryEntry::Program(Program::other()),
            ),
            (
                "example_applications".to_string(),
                DirectoryEntry::Directory(Directory {
                    entries: BTreeMap::from([(
                        "rust".to_string(),
                        DirectoryEntry::Directory(Directory {
                            entries: BTreeMap::from([
                                (
                                    "call_api".to_string(),
                                    DirectoryEntry::Program(Program::other()),
                                ),
                                (
                                    "database".to_string(),
                                    DirectoryEntry::Directory(Directory {
                                        entries: BTreeMap::from([
                                            (
                                                "database_client".to_string(),
                                                DirectoryEntry::Program(Program::other()),
                                            ),
                                            (
                                                "database_server".to_string(),
                                                DirectoryEntry::Program(Program::other()),
                                            ),
                                            (
                                                "database_trait".to_string(),
                                                DirectoryEntry::Program(Program::other()),
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
                                                DirectoryEntry::Program(Program::other()),
                                            ),
                                            (
                                                "essrpc_server".to_string(),
                                                DirectoryEntry::Program(Program::other()),
                                            ),
                                            (
                                                "essrpc_trait".to_string(),
                                                DirectoryEntry::Program(Program::other()),
                                            ),
                                        ]),
                                    }),
                                ),
                                (
                                    "hello_rust".to_string(),
                                    DirectoryEntry::Program(Program::other()),
                                ),
                                (
                                    "idle_service".to_string(),
                                    DirectoryEntry::Program(Program::other()),
                                ),
                                (
                                    "provide_api".to_string(),
                                    DirectoryEntry::Program(Program::other()),
                                ),
                            ]),
                        }),
                    )]),
                }),
            ),
            (
                "management_interface".to_string(),
                DirectoryEntry::Program(Program::other()),
            ),
            (
                "management_service".to_string(),
                DirectoryEntry::Program(match maybe_raspberry_pi {
                    Some(raspberry_pi) => Program::host_and_pi(raspberry_pi),
                    None => Program::host(),
                }),
            ),
            (
                "nonlocality_env".to_string(),
                DirectoryEntry::Program(Program::other()),
            ),
        ]),
    };

    error_count += build_and_test_recursively(&root, &repository).await;
    match error_count.0 {
        0 => std::process::ExitCode::SUCCESS,
        _ => {
            println!("{} errors.", error_count.0);
            std::process::ExitCode::FAILURE
        }
    }
}
