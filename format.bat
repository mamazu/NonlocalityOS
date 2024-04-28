@echo off
echo Formatting..

pushd downloader || exit /B 1
cargo fmt || exit /B 1
popd

pushd management_interface || exit /B 1
cargo fmt || exit /B 1
popd

pushd admin_tool || exit /B 1
cargo fmt || exit /B 1
popd

pushd management_service || exit /B 1
cargo fmt || exit /B 1
popd

pushd nonlocality_env || exit /B 1
cargo fmt || exit /B 1
popd

pushd example_applications\rust || exit /B 1
call .\format.bat || exit /B 1
popd
