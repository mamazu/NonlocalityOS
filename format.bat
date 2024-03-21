@echo off
pushd engine || exit /B 1
cargo fmt || exit /B 1
