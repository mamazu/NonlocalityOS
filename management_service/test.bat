@echo off
cargo test || exit /B 1
cargo build || exit /B 1
setlocal
