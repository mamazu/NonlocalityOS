@echo off
setlocal
set repository=%CD%

pushd downloader || exit /B 1
cargo build --release || exit /B 1

rem found this compiler on https://developer.arm.com/downloads/-/gnu-a
set compiler_name=gcc-arm-10.3-2021.07-mingw-w64-i686-aarch64-none-linux-gnu
set compiler_download_url=https://developer.arm.com/-/media/Files/downloads/gnu-a/10.3-2021.07/binrel/%compiler_name%.tar.xz?rev=06b6c36e428c48fda4b6d907f17308be^&hash=B36CC5C9544DCFCB2DB06FB46C8B8262
set compiler_archive_path=%repository%\tools\%compiler_name%.tar.xz
set compiler_unpack_dir=%repository%\tools\raspberry_pi_compiler
cargo run "%compiler_download_url%" "%compiler_archive_path%" "%compiler_unpack_dir%" || exit /B 1

popd

pushd engine || exit /B 1
cargo test || exit /B 1

rem build for the Raspberry Pi because compiling stuff on the device itself is very slow

rem this target is duplicated in engine/.cargo/config.toml
set raspberry_pi_target=aarch64-unknown-linux-gnu

rustup target add %raspberry_pi_target% || exit /B 1
set CC_aarch64-unknown-linux-gnu=%compiler_unpack_dir%\%compiler_name%\bin\aarch64-none-linux-gnu-gcc.exe
set AR_aarch64-unknown-linux-gnu=%compiler_unpack_dir%\%compiler_name%\bin\aarch64-none-linux-gnu-ar.exe
set LD_LIBRARY_PATH=%compiler_unpack_dir%\%compiler_name%\aarch64-none-linux-gnu\libc\lib64
cargo build --target %raspberry_pi_target% --config target.aarch64-unknown-linux-gnu.linker='%CC_aarch64-unknown-linux-gnu%' --release || exit /B 1

popd

pushd example_applications || exit /B 1
.\run_all.bat || exit /B 1

echo Success!
