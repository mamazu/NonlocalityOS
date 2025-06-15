use crate::run::ReportProgress;
use relative_path::{RelativePath, RelativePathBuf};
use ssh2::OpenFlags;
use std::{net::SocketAddr, pin::Pin, sync::Arc, time::Instant};
use tracing::{info, span, Level};

fn to_std_path(linux_path: &relative_path::RelativePath) -> std::path::PathBuf {
    linux_path.to_path(std::path::Path::new("/"))
}

fn format_bytes(size: u64) -> String {
    const SIZE_OF_BYTE: f64 = 1_000.0;
    let units: [&str; 6] = ["B", "KB", "MB", "GB", "TB", "PB"];
    let scale = std::cmp::min(
        units.len() - 1,
        (size as f64).log(SIZE_OF_BYTE).floor() as usize,
    );
    let unit = units[scale];
    let scaled_size = size as f64 / SIZE_OF_BYTE.powf(scale as f64);
    format!("{scaled_size:.1} {unit}")
}

#[cfg(test)]
#[test_log::test]
fn test_format() {
    assert_eq!("10.0 B", format_bytes(10));
    assert_eq!("1.0 KB", format_bytes(1000));
    assert_eq!("1.2 KB", format_bytes(1200));
    assert_eq!("1.3 GB", format_bytes(1298783830));

    // Testing that large file sizes don't crash
    assert_eq!("12000.0 PB", format_bytes(12000000000000000000));
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
    info!("Uploading file with {}", format_bytes(file_size));

    let mode = match is_executable {
        true => 0o755,
        false => 0o644,
    };
    let before_upload = Instant::now();
    let mut file_uploader = sftp
        .open_mode(
            to_std_path(to),
            OpenFlags::WRITE | OpenFlags::TRUNCATE,
            mode,
            ssh2::OpenType::File,
        )
        .expect("Tried to create binary on the remote");
    std::io::copy(&mut file_to_upload, &mut file_uploader)
        .expect("Tried to upload the file contents");
    std::io::Write::flush(&mut file_uploader).expect("Tried to flush file uploader");
    drop(file_uploader);
    let after_upload = Instant::now();
    let upload_duration = after_upload.duration_since(before_upload);
    info!("Upload duration: {:.1} s", upload_duration.as_secs_f64());

    let upload_speed_bytes_per_second = file_size as f64 / upload_duration.as_secs_f64();
    info!(
        "Upload speed: {}/s",
        format_bytes(upload_speed_bytes_per_second as u64)
    );

    let mut channel = session.channel_session().unwrap();
    channel.exec(&format!("file {to}")).unwrap();
    let mut standard_output = String::new();
    std::io::Read::read_to_string(&mut channel, &mut standard_output)
        .expect("Tried to read standard output");
    info!("file {}", standard_output.trim());
    channel.wait_close().expect("Waited for close");
    assert_eq!(0, channel.exit_status().unwrap());
}

async fn run_simple_ssh_command(session: &ssh2::Session, command: &str) {
    let span = span!(Level::INFO, "SSH", command = command);
    let _enter = span.enter();
    let mut channel: ssh2::Channel = session.channel_session().unwrap();
    channel.exec(command).expect("Tried exec");

    let mut standard_output = String::new();
    let standard_output_stream_id = 0;
    std::io::Read::read_to_string(
        &mut channel.stream(standard_output_stream_id),
        &mut standard_output,
    )
    .expect("Tried to read standard output");
    if !standard_output.is_empty() {
        info!("Standard output:\n{}", standard_output.trim_end());
    }

    let mut standard_error = String::new();
    let standard_error_stream_id = ssh2::EXTENDED_DATA_STDERR;
    std::io::Read::read_to_string(
        &mut channel.stream(standard_error_stream_id),
        &mut standard_error,
    )
    .expect("Tried to read standard error");
    if !standard_error.is_empty() {
        info!("Standard error:\n{}", standard_error.trim_end());
    }

    channel.wait_close().expect("Waited for close");
    let exit_code = channel.exit_status().unwrap();
    info!("Exit code: {}", exit_code);
    assert_eq!(0, exit_code, "Expected exit code for success");
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
                "32-bit ARM systems such as the Raspberry Pi 2 are currently not supported"
                    .to_string(),
            ))
        }
        _ => {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Unknown architecture: {trimmed}"),
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

struct DeploymentSession {
    session: ssh2::Session,
    nonlocality_dir: RelativePathBuf,
    remote_host_binary: RelativePathBuf,
}

impl DeploymentSession {
    fn new(
        session: ssh2::Session,
        nonlocality_dir: RelativePathBuf,
        remote_host_binary: RelativePathBuf,
    ) -> Self {
        Self {
            session,
            nonlocality_dir,
            remote_host_binary,
        }
    }
}

async fn deploy_host_binary(
    build_host_binary: Box<BuildHostBinary>,
    host_binary_name: &str,
    ssh_endpoint: &SocketAddr,
    ssh_user: &str,
    ssh_password: &str,
    progress_reporter: &Arc<dyn ReportProgress + Sync + Send>,
) -> std::io::Result<DeploymentSession> {
    info!("Connecting to {}", &ssh_endpoint);
    let tcp = std::net::TcpStream::connect(ssh_endpoint).unwrap();
    let mut session = ssh2::Session::new().unwrap();
    session.set_tcp_stream(tcp);
    match session.handshake() {
        Ok(_) => {}
        Err(error) => progress_reporter.log(&format!("Could not SSH handshake: {error}")),
    }

    info!("Authenticating as {} using password", &ssh_user);
    session.userauth_password(ssh_user, ssh_password).unwrap();
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
            format!("Expected a directory at remote location {home}"),
        ));
    }

    let nonlocality_dir = home.join(".nonlocality");
    match sftp.stat(&to_std_path(&nonlocality_dir)) {
        Ok(exists) => {
            if exists.is_dir() {
                info!(
                    "The nonlocality directory already exists: {}",
                    nonlocality_dir
                );
            } else {
                let message = format!("{nonlocality_dir} exists, but is not a directory");
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    message,
                ));
            }
        }
        Err(error) => {
            info!("Could not stat nonlocality directory: {}", error);
            info!("Creating nonlocality directory {}", nonlocality_dir);
            sftp.mkdir(&to_std_path(&nonlocality_dir), 0o755)
                .expect("Tried to create nonlocality directory on the remote");
        }
    }

    let temporary_directory = tempfile::tempdir().unwrap();
    let host_binary = temporary_directory.path().join(host_binary_name);
    build_host_binary(&host_binary, &remote_build_target, progress_reporter).await?;

    let remote_host_binary_next = nonlocality_dir.join(format!("{host_binary_name}.next"));
    upload_file(
        &session,
        &sftp,
        &host_binary,
        &remote_host_binary_next,
        true,
    );
    drop(host_binary);
    drop(temporary_directory);

    let remote_host_binary = nonlocality_dir.join(host_binary_name);
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
    Ok(DeploymentSession::new(
        session,
        nonlocality_dir,
        remote_host_binary,
    ))
}

#[allow(clippy::too_many_arguments)]
pub async fn deploy(
    build_host_binary: Box<BuildHostBinary>,
    host_binary_name: &str,
    ssh_endpoint: &SocketAddr,
    ssh_user: &str,
    ssh_password: &str,
    progress_reporter: &Arc<dyn ReportProgress + Sync + Send>,
) -> std::io::Result<()> {
    let deployment_session = deploy_host_binary(
        build_host_binary,
        host_binary_name,
        ssh_endpoint,
        ssh_user,
        ssh_password,
        progress_reporter,
    )
    .await?;

    info!("Starting the host binary on the remote to install itself as a service.");
    let sudo = RelativePath::new("/usr/bin/sudo");
    run_simple_ssh_command(
        &deployment_session.session,
        &format!(
            "{} '{}' install '{}'",
            sudo, deployment_session.remote_host_binary, deployment_session.nonlocality_dir
        ),
    )
    .await;

    Ok(())
}

pub async fn uninstall(
    build_host_binary: Box<BuildHostBinary>,
    host_binary_name: &str,
    ssh_endpoint: &SocketAddr,
    ssh_user: &str,
    ssh_password: &str,
    progress_reporter: &Arc<dyn ReportProgress + Sync + Send>,
) -> std::io::Result<()> {
    let deployment_session = deploy_host_binary(
        build_host_binary,
        host_binary_name,
        ssh_endpoint,
        ssh_user,
        ssh_password,
        progress_reporter,
    )
    .await?;

    info!("Starting the host binary on the remote to uninstall its service.");
    let sudo = RelativePath::new("/usr/bin/sudo");
    run_simple_ssh_command(
        &deployment_session.session,
        &format!(
            "{} '{}' uninstall",
            sudo, deployment_session.remote_host_binary
        ),
    )
    .await;

    Ok(())
}
