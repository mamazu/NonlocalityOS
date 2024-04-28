@echo off

echo database
pushd database || exit /B 1
call .\test.bat || exit /B 1
popd

echo essrpc_example
pushd essrpc_example || exit /B 1
call .\test.bat || exit /B 1
popd

echo hello_rust
pushd hello_rust || exit /B 1
call .\test.bat || exit /B 1
popd

echo idle_service
pushd idle_service || exit /B 1
call .\test.bat || exit /B 1
popd

echo provide_api
pushd provide_api || exit /B 1
call .\test.bat || exit /B 1
popd

echo call_api
pushd call_api || exit /B 1
call .\test.bat || exit /B 1
popd
