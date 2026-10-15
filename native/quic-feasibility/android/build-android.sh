#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

android_sdk="${ANDROID_SDK_ROOT:-/home/unmbp/Android/Sdk}"
android_ndk="${ANDROID_NDK_HOME:-$android_sdk/ndk/28.2.13676358}"
android_llvm="$android_ndk/toolchains/llvm/prebuilt/linux-x86_64/bin"
export CARGO_TARGET_AARCH64_LINUX_ANDROID_LINKER="$android_llvm/aarch64-linux-android24-clang"
export CARGO_TARGET_X86_64_LINUX_ANDROID_LINKER="$android_llvm/x86_64-linux-android24-clang"
export CC_aarch64_linux_android="$CARGO_TARGET_AARCH64_LINUX_ANDROID_LINKER"
export CC_x86_64_linux_android="$CARGO_TARGET_X86_64_LINUX_ANDROID_LINKER"
export AR_aarch64_linux_android="$android_llvm/llvm-ar"
export AR_x86_64_linux_android="$android_llvm/llvm-ar"
export CARGO_TARGET_AARCH64_LINUX_ANDROID_RUSTFLAGS="-C link-arg=-Wl,-z,max-page-size=16384 -C link-arg=-Wl,--no-undefined"
export CARGO_TARGET_X86_64_LINUX_ANDROID_RUSTFLAGS="-C link-arg=-Wl,-z,max-page-size=16384 -C link-arg=-Wl,--no-undefined"

cargo build --locked --release --lib --target aarch64-linux-android --target x86_64-linux-android
mkdir -p build/aar/jni/arm64-v8a build/aar/jni/x86_64 build/classes build/headers
cp target/aarch64-linux-android/release/libt3_quic_probe.so build/aar/jni/arm64-v8a/
cp target/x86_64-linux-android/release/libt3_quic_probe.so build/aar/jni/x86_64/
javac --release 8 -h build/headers -d build/classes java/dev/t3/quicprobe/NativeProbe.java
jar cf build/aar/classes.jar -C build/classes .
cat > build/aar/AndroidManifest.xml <<'EOF'
<manifest xmlns:android="http://schemas.android.com/apk/res/android" package="dev.t3.quicprobe">
  <uses-sdk android:minSdkVersion="24" />
  <uses-permission android:name="android.permission.INTERNET" />
</manifest>
EOF
cat > build/aar/proguard.txt <<'EOF'
-keep class dev.t3.quicprobe.NativeProbe { *; }
EOF
python3 - <<'PY'
from pathlib import Path
from zipfile import ZipFile, ZIP_DEFLATED
root = Path("build/aar")
with ZipFile("build/t3-quic-probe.aar", "w", ZIP_DEFLATED) as archive:
    for path in sorted(root.rglob("*")):
        if path.is_file():
            archive.write(path, path.relative_to(root))
PY
"$android_llvm/llvm-readelf" -h -l -d build/aar/jni/arm64-v8a/libt3_quic_probe.so
"$android_llvm/llvm-readelf" -h -l -d build/aar/jni/x86_64/libt3_quic_probe.so
ls -lh build/t3-quic-probe.aar
