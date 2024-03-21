@echo off
cargo test || exit /B 1
cargo build --target wasm32-wasi || exit /B 1
setlocal
set "example_dir=%CD%"
pushd ..\..\..\engine || exit /B 1
cargo run %example_dir%\target\wasm32-wasi\debug\hello_rust.wasm || exit /B 1
popd
