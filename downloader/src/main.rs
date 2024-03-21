use curl::easy::Easy;
use std::env;
use std::fs;
use std::fs::File;
use std::io::Write;
use tar::Archive;
use xz2::read::XzDecoder;

fn download(download_url: &str, download_file_path: &str) -> Result<(), std::io::Error> {
    let mut easy = Easy::new();
    easy.url(download_url).unwrap();
    easy.follow_location(true)?;
    println!("Creating file {}.", download_file_path);
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

fn unpack(archive_path: &str, unpack_destination_directory: &str) -> Result<(), std::io::Error> {
    println!(
        "Unpacking compressed archive {} into {}.",
        archive_path, unpack_destination_directory
    );
    let file = File::open(archive_path)?;
    let decompressor = XzDecoder::new_multi_decoder(file);
    let mut archive = Archive::new(decompressor);
    archive.unpack(unpack_destination_directory)?;
    println!("Unpacking completed.");
    Ok(())
}

fn main() -> Result<(), std::io::Error> {
    let args: Vec<String> = env::args().collect();
    let download_url = &args[1];
    let download_file_path = &args[2];
    let unpack_destination_directory = &args[3];

    if let Ok(metadata) = fs::metadata(unpack_destination_directory) {
        if metadata.is_dir() {
            println!(
                "Directory '{}' already exists.",
                unpack_destination_directory
            );
            return Ok(());
        } else {
            println!(
                "Path '{}' exists but is not a directory.",
                unpack_destination_directory
            );
            panic!("TO DO");
        }
    } else {
        println!(
            "Directory '{}' does not exist.",
            unpack_destination_directory
        );
    }

    if let Ok(metadata) = fs::metadata(download_file_path) {
        if metadata.is_file() {
            println!("File '{}' exists.", download_file_path);
        } else {
            println!("Path '{}' exists but is not a file.", download_file_path);
            panic!("TO DO");
        }
    } else {
        println!("File '{}' does not exist.", download_file_path);
        download(&download_url, &download_file_path)?;
    }

    unpack(&download_file_path, &unpack_destination_directory)?;
    Ok(())
}
