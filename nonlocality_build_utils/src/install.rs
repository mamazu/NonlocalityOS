use crate::run::ReportProgress;
use relative_path::RelativePath;
use ssh2::OpenFlags;
use std::{net::SocketAddr, pin::Pin, sync::Arc};
use tracing::info;

pub const NONLOCALITY_HOST_BINARY_NAME: &str = "nonlocality_host";

fn to_std_path(linux_path: &relative_path::RelativePath) -> std::path::PathBuf {
    linux_path.to_path(std::path::Path::new("/"))
}

fn upload_file(
    session: &ssh2::Session,
    sftp: &ssh2::Sftp,
    from: &std::path::Path,
    to: &RelativePath,
    is_executable: bool,
) {
    info!("Uploading {} to {}", from.display(), to);
    let mut file_to_upload = std::fs::File::open(from).expect("Tried to open the binary to upload");
    let file_size = file_to_upload
        .metadata()
        .expect("Tried to determine the file size")
        .len();
    info!("Uploading file with {} bytes", file_size);

    let mode = match is_executable {
        true => 0o755,
        false => 0o644,
    };
    let mut file_uploader = sftp
        .open_mode(
            &to_std_path(to),
            OpenFlags::WRITE | OpenFlags::TRUNCATE,
            mode,
            ssh2::OpenType::File,
        )
        .expect("Tried to create binary on the remote");
    std::io::copy(&mut file_to_upload, &mut file_uploader)
        .expect("Tried to upload the file contents");
    std::io::Write::flush(&mut file_uploader).expect("Tried to flush file uploader");
    drop(file_uploader);

    let mut channel = session.channel_session().unwrap();
    channel.exec(&format!("file {}", to)).unwrap();
    let mut standard_output = String::new();
    std::io::Read::read_to_string(&mut channel, &mut standard_output)
        .expect("Tried to read standard output");
    info!("{}", standard_output);
    channel.wait_close().expect("Waited for close");
    assert_eq!(0, channel.exit_status().unwrap());
}

async fn run_simple_ssh_command(session: &ssh2::Session, command: &str) {
    info!("Running {}", command);
    let mut channel: ssh2::Channel = session.channel_session().unwrap();
    channel.exec(command).expect("Tried exec");

    let mut standard_output = String::new();
    let standard_output_stream_id = 0;
    std::io::Read::read_to_string(
        &mut channel.stream(standard_output_stream_id),
        &mut standard_output,
    )
    .expect("Tried to read standard output");
    info!("Standard output: {}", standard_output);

    let mut standard_error = String::new();
    let standard_error_stream_id = ssh2::EXTENDED_DATA_STDERR;
    std::io::Read::read_to_string(
        &mut channel.stream(standard_error_stream_id),
        &mut standard_error,
    )
    .expect("Tried to read standard error");
    info!("Standard error: {}", standard_error);

    channel.wait_close().expect("Waited for close");
    let exit_code = channel.exit_status().unwrap();
    info!("Exit code: {}", exit_code);
    assert_eq!(0, exit_code);
}

#[derive(Clone, Debug)]
pub enum BuildTarget {
    LinuxAmd64,
    RaspberryPi64,
}

fn detect_remote_build_target(session: &ssh2::Session) -> std::io::Result<BuildTarget> {
    let mut channel = session.channel_session().unwrap();
    let command = "/bin/bash -c 'uname -m'";
    info!("SSH command '{}'", command);
    channel.exec(command)?;
    let mut standard_output = String::new();
    std::io::Read::read_to_string(&mut channel, &mut standard_output)?;
    channel.wait_close()?;
    let exit_code = channel.exit_status()?;
    info!("SSH command '{}' exited with {}", command, exit_code);
    info!(
        "SSH command '{}' standard output: {}",
        command, &standard_output
    );
    assert_eq!(0, exit_code);
    let trimmed = standard_output.trim();
    let target = match trimmed {
        "x86_64" => BuildTarget::LinuxAmd64,
        "aarch64" => BuildTarget::RaspberryPi64,
        "armv7l" => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "32-bit ARM systems such as the Raspberry Pi 2 are currently not supported"
                ),
            ))
        }
        _ => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Unknown architecture: {}", trimmed),
            ))
        }
    };
    info!("Remote target detected: {:?}", &target);
    Ok(target)
}

pub type BuildHostBinary = dyn FnOnce(
    &std::path::Path,
    &BuildTarget,
    &Arc<dyn ReportProgress + Sync + Send>,
) -> Pin<
    Box<dyn std::future::Future<Output = std::io::Result<()>> + Sync + Send>,
>;

pub const INITIAL_DATABASE_FILE_NAME: &str = "initial_database.sqlite3";

pub async fn deploy(
    initial_database: &std::path::Path,
    build_host_binary: Box<BuildHostBinary>,
    ssh_endpoint: &SocketAddr,
    ssh_user: &str,
    ssh_password: &str,
    progress_reporter: &Arc<dyn ReportProgress + Sync + Send>,
) -> std::io::Result<()> {
    info!("Connecting to {}", &ssh_endpoint);
    let tcp = std::net::TcpStream::connect(&ssh_endpoint).unwrap();
    let mut session = ssh2::Session::new().unwrap();
    session.set_tcp_stream(tcp);
    match session.handshake() {
        Ok(_) => {}
        Err(error) => progress_reporter.log(&format!("Could not SSH handshake: {}", error)),
    }

    info!("Authenticating as {} using password", &ssh_user);
    session.userauth_password(&ssh_user, &ssh_password).unwrap();
    assert!(session.authenticated());
    info!("Authenticated as {}", &ssh_user);

    let remote_build_target = detect_remote_build_target(&session).unwrap();

    let sftp = session.sftp().expect("Tried to open SFTP");
    let home = relative_path::RelativePath::new("/home").join(ssh_user);
    let home_found = sftp
        .stat(&to_std_path(&home))
        .expect("Tried to stat home on the remote");
    if !home_found.is_dir() {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("Expected a directory at remote location {}", home),
        ));
    }

    let nonlocality_dir = home.join(".nonlocality");
    match sftp.stat(&to_std_path(&nonlocality_dir)) {
        Ok(exists) => {
            if exists.is_dir() {
                info!("Our directory appears to exist.");
            } else {
                let message = format!(
                    "Our directory {} exists, but is not a directory",
                    nonlocality_dir
                );
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    message,
                ));
            }
        }
        Err(error) => {
            info!("Could not stat our directory: {}", error);
            info!("Creating directory {}", nonlocality_dir);
            sftp.mkdir(&to_std_path(&nonlocality_dir), 0o755)
                .expect("Tried to create our directory on the remote");
        }
    }

    let temporary_directory = tempfile::tempdir().unwrap();
    let host_binary = temporary_directory
        .path()
        .join(NONLOCALITY_HOST_BINARY_NAME);
    build_host_binary(&host_binary, &remote_build_target, progress_reporter).await?;

    let remote_host_binary_next =
        nonlocality_dir.join(format!("{}.next", NONLOCALITY_HOST_BINARY_NAME));
    upload_file(
        &session,
        &sftp,
        &host_binary,
        &remote_host_binary_next,
        true,
    );
    drop(host_binary);
    drop(temporary_directory);

    let remote_host_binary = nonlocality_dir.join(NONLOCALITY_HOST_BINARY_NAME);
    // Sftp.rename doesn't work (error "4", and it's impossible to find documentation on what "4" means).
    run_simple_ssh_command(
        &session,
        // TODO: encode command line arguments correctly
        &format!(
            "/usr/bin/mv {} {}",
            &remote_host_binary_next, &remote_host_binary
        ),
    )
    .await;

    let remote_database = nonlocality_dir.join(INITIAL_DATABASE_FILE_NAME);
    upload_file(&session, &sftp, initial_database, &remote_database, false);

    let sudo = RelativePath::new("/usr/bin/sudo");
    run_simple_ssh_command(
        &session,
        &format!("{} {} install", sudo, remote_host_binary),
    )
    .await;

    Ok(())
}
