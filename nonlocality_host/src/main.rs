use std::path::Path;

use tracing::{error, info};
use tracing_subscriber::fmt::format::FmtSpan;

async fn handle_command_line(
    nonlocality_directory: &Path,
    command_line_arguments: &[String],
) -> std::io::Result<()> {
    info!("Command line arguments: {:?}", command_line_arguments);
    if command_line_arguments.len() != 1 {
        error!("Command line argument required: install|run");
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            "Invalid number of command line arguments",
        ));
    }
    let command = command_line_arguments[0].as_str();
    match command {
        "install" => {
            info!("Installing host in {}", nonlocality_directory.display());
            Ok(())
        }
        "run" => {
            info!("Running host in {}", nonlocality_directory.display());
            Ok(())
        }
        _ => Err(std::io::Error::new(
            std::io::ErrorKind::InvalidInput,
            format!("Unknown command {}", command),
        )),
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_install_host() -> std::io::Result<()> {
    let temporary_directory = tempfile::tempdir()?;
    handle_command_line(temporary_directory.path(), &["install".to_string()]).await
}

#[tokio::test(flavor = "multi_thread")]
async fn test_run_host() -> std::io::Result<()> {
    let temporary_directory = tempfile::tempdir()?;
    handle_command_line(temporary_directory.path(), &["run".to_string()]).await
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> std::io::Result<()> {
    tracing_subscriber::fmt()
        .with_span_events(FmtSpan::CLOSE)
        .init();
    let command_line_arguments: Vec<String> = std::env::args().collect();
    let current_binary = Path::new(&command_line_arguments[0]);
    info!("Current binary: {}", current_binary.display());
    let nonlocality_directory = current_binary.parent().unwrap();
    info!(
        "Nonlocality directory detected: {}",
        nonlocality_directory.display()
    );
    handle_command_line(nonlocality_directory, &command_line_arguments[1..]).await
}
