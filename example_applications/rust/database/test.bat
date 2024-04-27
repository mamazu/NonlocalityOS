@echo off
pushd database_trait || exit /B 1
call .\test.bat || exit /B 1
popd

pushd database_client || exit /B 1
call .\test.bat || exit /B 1
popd

pushd database_server || exit /B 1
call .\test.bat || exit /B 1
popd
