@echo off
pushd essrpc_trait || exit /B 1
call .\test.bat || exit /B 1
popd

pushd essrpc_client || exit /B 1
call .\test.bat || exit /B 1
popd

pushd essrpc_server || exit /B 1
call .\test.bat || exit /B 1
popd