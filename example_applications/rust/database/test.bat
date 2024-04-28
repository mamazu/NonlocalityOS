@echo off
echo database_trait
pushd database_trait || exit /B 1
call .\test.bat || exit /B 1
popd

echo database_client
pushd database_client || exit /B 1
call .\test.bat || exit /B 1
popd

echo database_server
pushd database_server || exit /B 1
call .\test.bat || exit /B 1
popd
