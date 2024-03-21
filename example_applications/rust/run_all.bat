@echo off
pushd rust_example || exit /B 1
cargo build --target wasm32-wasi || exit /B 1
setlocal
set "example_dir=%CD%"
popd || exit /B 1
pushd ..\..\engine || exit /B 1
cargo run %example_dir%\target\wasm32-wasi\debug\rust_example.wasm || exit /B 1
