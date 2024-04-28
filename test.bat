cls || exit /B 1
call .\astra.bat || exit /B 1

setlocal
set repository=%CD%

set rpi_compiler_name=gcc-arm-10.3-2021.07-mingw-w64-i686-aarch64-none-linux-gnu
set rpi_compiler_unpack_dir=%repository%\tools\raspberry_pi_compiler

set wasi_compiler_name=wasi-sdk-22
set wasi_compiler_unpack_dir=%repository%\tools\%wasi_compiler_name%.0.m-mingw
set CC_wasm32-wasi=%wasi_compiler_unpack_dir%\%wasi_compiler_name%.0+m\bin\clang.exe
set CC_wasm32-wasip1-threads=%CC_wasm32-wasi%

rustup toolchain install nightly-x86_64-pc-windows-msvc || exit /B 1

pushd example_applications || exit /B 1
call .\test.bat || exit /B 1
popd

echo Success!
