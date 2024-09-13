use std::{thread, time};

fn main() {
    println!("Doing nothing..");
    thread::sleep(time::Duration::from_secs(3));
    println!("Done!");
}
