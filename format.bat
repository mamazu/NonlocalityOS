@echo off
echo Formatting..

pushd engine || exit /B 1
cargo fmt || exit /B 1
popd

pushd downloader || exit /B 1
cargo fmt || exit /B 1
popd

pushd management_service || exit /B 1
cargo fmt || exit /B 1
popd

pushd example_applications\rust\hello_rust || exit /B 1
cargo fmt || exit /B 1
popd

pushd example_applications\rust\idle_service || exit /B 1
cargo fmt || exit /B 1
popd

pushd example_applications\rust\call_api || exit /B 1
cargo fmt || exit /B 1
popd
