@echo off
pushd hello_rust || exit /B 1
call .\test.bat || exit /B 1
popd

pushd idle_service || exit /B 1
call .\test.bat || exit /B 1
popd

pushd provide_api || exit /B 1
call .\test.bat || exit /B 1
popd

pushd call_api || exit /B 1
call .\test.bat || exit /B 1
popd

pushd capnproto_server || exit /B 1
call .\test.bat || exit /B 1
popd

pushd capnproto_client || exit /B 1
call .\test.bat || exit /B 1
popd
