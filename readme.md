# Setup

* install Rust: <https://www.rust-lang.org/learn/get-started> (tested with Rust 1.84.0)
* restart your terminal
* install the `coverage-gutters` VS Code extension <https://marketplace.visualstudio.com/items?itemName=ryanluker.vscode-coverage-gutters>

## Windows

* install Visual Studio 2022 including the C++ toolchain
  * possibly, the [Build Tools](https://aka.ms/vs/17/release/vs_BuildTools.exe) are sufficient
* install <https://strawberryperl.com/> (tested with *5.40.0.1 (2024-08-10)*)
* restart your terminal

## Linux Mint / Ubuntu / Debian

* `sudo apt install build-essential libssl-dev podman`
* restart your terminal

# Test

* run `./test.sh`
* on success it will print `Success!` in the end

# Watch code and keep running tests

## Linux

* `./watch_cargo_tests.sh`

## Windows

* `watch_cargo_tests.bat`

# Coverage

## Linux

* run `./coverage.sh`

## Windows

* run `coverage.bat`

# Formatting code

## Linux

* run `./test.sh` or `./build.sh`

## Windows

* run `test.bat` or `build.bat`

## Visual Studio Code

* install the `rust-analyzer` VS Code extension
