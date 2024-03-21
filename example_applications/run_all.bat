@echo off
pushd rust || exit /B 1
rustup target add wasm32-wasi || exit /B 1
.\run_all.bat || exit /B 1
