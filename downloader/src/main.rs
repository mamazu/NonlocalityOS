use curl::easy::Easy;
use flate2::read::GzDecoder;
use std::env;
use std::fs;
use std::fs::File;
use std::io;
use std::io::Write;
use std::path::Path;
use tar::Archive;
use xz2::read::XzDecoder;

fn download(download_url: &str, download_file_path: &Path) -> Result<(), std::io::Error> {
    let mut easy = Easy::new();
    easy.url(download_url).unwrap();
    easy.follow_location(true)?;
    println!("Creating file {}.", download_file_path.display());
    let mut file = File::create(download_file_path)?;
    {
        println!("Downloading from {}.", download_url);
        let mut transfer = easy.transfer();
        transfer
            .write_function(|data| match file.write_all(data) {
                Ok(_) => Ok(data.len()),
                Err(_) => Ok(0),
            })
            .unwrap();
        transfer.perform().unwrap();
    }
    file.flush()?;
    println!("Download completed.");
    Ok(())
}

fn unpack(
    archive_path: &Path,
    unpack_destination_directory: &Path,
    make_decoder: fn(File) -> io::Result<Box<dyn std::io::Read>>,
) -> Result<(), std::io::Error> {
    let temp_unpack_destination_directory_string = format!(
        "{}.temp",
        unpack_destination_directory
            .to_str()
            .expect("Expected UTF-8 compatible path")
    );
    let temp_unpack_destination_directory = Path::new(&temp_unpack_destination_directory_string);

    match std::fs::metadata(&temp_unpack_destination_directory) {
        Ok(_) => {
            println!(
                "Deleting {} from a previous run.",
                &temp_unpack_destination_directory.display()
            );
            std::fs::remove_dir_all(&temp_unpack_destination_directory)?
        }
        Err(_) => {}
    }

    println!(
        "Unpacking compressed archive {} into {}.",
        archive_path.display(),
        temp_unpack_destination_directory.display()
    );
    let file = File::open(archive_path)?;
    let decompressor = make_decoder(file)?;
    let mut archive = Archive::new(decompressor);
    archive.unpack(temp_unpack_destination_directory)?;
    println!("Unpacking completed.");

    println!(
        "Renaming unpacked directory from {} to {}.",
        temp_unpack_destination_directory.display(),
        unpack_destination_directory.display()
    );
    std::fs::rename(
        &temp_unpack_destination_directory,
        &unpack_destination_directory,
    )?;
    Ok(())
}

fn main() -> Result<(), std::io::Error> {
    let args: Vec<String> = env::args().collect();
    let download_url = &args[1];
    let download_file_path = Path::new(&args[2]);
    let unpack_destination_directory = Path::new(&args[3]);
    let archive_type = &args[4];

    if let Ok(metadata) = fs::metadata(unpack_destination_directory) {
        if metadata.is_dir() {
            println!(
                "Directory '{}' already exists.",
                unpack_destination_directory.display()
            );
            return Ok(());
        } else {
            panic!(
                "Path '{}' exists but is not a directory.",
                unpack_destination_directory.display()
            );
        }
    } else {
        println!(
            "Directory '{}' does not exist.",
            unpack_destination_directory.display()
        );
    }

    if let Ok(metadata) = fs::metadata(download_file_path) {
        if metadata.is_file() {
            println!("File '{}' exists.", download_file_path.display());
        } else {
            panic!(
                "Path '{}' exists but is not a file.",
                download_file_path.display()
            );
        }
    } else {
        println!("File '{}' does not exist.", download_file_path.display());
        download(&download_url, download_file_path)?;
    }

    match archive_type.as_str() {
        "tar.xz" => unpack(download_file_path, unpack_destination_directory, |input| {
            Ok(Box::new(XzDecoder::new_multi_decoder(input)))
        })?,
        "tar.gz" => unpack(download_file_path, unpack_destination_directory, |input| {
            Ok(Box::new(GzDecoder::new(input)))
        })?,
        _ => panic!("Unknown archive type: {}.", archive_type),
    }
    Ok(())
}
