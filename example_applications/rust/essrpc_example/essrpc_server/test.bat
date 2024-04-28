@echo off
cargo +nightly build --target wasm32-wasip1-threads || exit /B 1
