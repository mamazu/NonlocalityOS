@echo off
rem cargo test || exit /B 1
cargo build --target wasm32-wasip1-threads || exit /B 1
