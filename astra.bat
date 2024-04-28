setlocal

set repository=%~dp0

pushd astra || exit /B 1
cargo run --release %repository% || exit /B 1
popd

endlocal
