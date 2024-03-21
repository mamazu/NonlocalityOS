use std::{thread, time};

fn main() {
    loop {
        println!("Doing nothing..");
        thread::sleep(time::Duration::from_secs(30));
    }
}
