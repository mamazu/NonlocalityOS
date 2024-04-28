@echo off
echo test
cargo test || exit /B 1

echo build wasm
cargo build --target wasm32-wasi || exit /B 1
