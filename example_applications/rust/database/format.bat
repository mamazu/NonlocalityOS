@echo off

pushd database_trait || exit /B 1
cargo fmt || exit /B 1
popd

pushd database_server || exit /B 1
cargo fmt || exit /B 1
popd

pushd database_client || exit /B 1
cargo fmt || exit /B 1
popd
