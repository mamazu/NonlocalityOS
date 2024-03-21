@echo off
pushd engine || exit /B 1
cargo fmt || exit /B 1
popd

pushd downloader || exit /B 1
cargo fmt || exit /B 1
popd

pushd example_applications\rust\rust_example || exit /B 1
cargo fmt || exit /B 1
popd
