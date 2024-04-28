#![deny(warnings)]
use async_recursion::async_recursion;
use std::collections::BTreeMap;

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

async fn build_and_test_program(
    _program: &Program,
    where_in_filesystem: &std::path::Path,
) -> NumberOfErrors {
    let maybe_output = tokio::process::Command::new("cargo")
        .args(&["fmt"])
        .current_dir(&where_in_filesystem)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true)
        .output()
        .await;
    let output = match maybe_output {
        Ok(output) => output,
        Err(error) => {
            println!("Failed to spawn cargo process: {}.", error);
            return NumberOfErrors(1);
        }
    };
    if output.status.success() {
        println!("Formatted: {}", where_in_filesystem.display());
        return NumberOfErrors(0);
    }
    println!(
        "cargo fmt in {} failed with exit status {}.",
        where_in_filesystem.display(),
        output.status
    );
    println!("Standard output:");
    let stdout = String::from_utf8_lossy(&output.stdout);
    println!("{}", &stdout);
    println!("Standard error:");
    let stderr = String::from_utf8_lossy(&output.stderr);
    println!("{}", &stderr);
    NumberOfErrors(1)
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
                "downloader".to_string(),
                DirectoryEntry::Program(Program {}),
            ),
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

    let error_count = build_and_test_recursively(&root, &repository).await;
    match error_count.0 {
        0 => std::process::ExitCode::SUCCESS,
        _ => {
            println!("{} errors.", error_count.0);
            std::process::ExitCode::FAILURE
        }
    }
}
