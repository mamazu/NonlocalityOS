@echo off
pushd engine || exit /B 1
cargo test || exit /B 1
cargo run || exit /B 1
echo Success!
