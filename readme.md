# Setup

* install Visual Studio with C++ or whatever
* install Rust: https://www.rust-lang.org/learn/get-started (tested with Rust 1.77.0)

# Test

* run `test.bat`
* on success it will print `Success!` in the end

# Deploy

* successfully run `.\build.bat`
* install Raspberry Pi OS 64-bit and enable SSH with password
* copy `.env.template` as `.env`
* edit `.env` with values for your Raspberry Pi!
* run `.\astra.bat deploy`
* enjoy

# Formatting code

## Windows

* run `test.bat` or `build.bat`

## Visual Studio Code

* install the `rust-analyzer` extension
