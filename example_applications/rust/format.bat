

pushd database || exit /B 1
call .\format.bat || exit /B 1
popd

pushd essrpc_example || exit /B 1
call .\format.bat || exit /B 1
popd

pushd hello_rust || exit /B 1
cargo fmt || exit /B 1
popd

pushd idle_service || exit /B 1
cargo fmt || exit /B 1
popd

pushd provide_api || exit /B 1
cargo fmt || exit /B 1
popd

pushd call_api || exit /B 1
cargo fmt || exit /B 1
popd
