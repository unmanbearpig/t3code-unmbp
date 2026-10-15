#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"
toolchain=${T3_PROBE_RUST_TOOLCHAIN:-1.96.1}
export PATH="$HOME/.cargo/bin:$PATH"
export IPHONEOS_DEPLOYMENT_TARGET=18.0
export MACOSX_DEPLOYMENT_TARGET=15.0
export TMPDIR="$HOME/codex-tmp/t3-apple-quic-build-tmp"
mkdir -p "$TMPDIR"
rustup target add --toolchain "$toolchain" aarch64-apple-ios aarch64-apple-ios-sim
for target in aarch64-apple-darwin aarch64-apple-ios aarch64-apple-ios-sim; do
  cargo "+$toolchain" build --locked --release --target "$target"
done
for platform in macosx iphoneos iphonesimulator; do
  case "$platform" in
    macosx) rust_target=aarch64-apple-darwin; swift_target=arm64-apple-macosx15.0 ;;
    iphoneos) rust_target=aarch64-apple-ios; swift_target=arm64-apple-ios18.0 ;;
    iphonesimulator) rust_target=aarch64-apple-ios-sim; swift_target=arm64-apple-ios18.0-simulator ;;
  esac
  xcrun --sdk "$platform" swiftc -target "$swift_target" -sdk "$(xcrun --sdk "$platform" --show-sdk-path)" \
    -import-objc-header Probe.h Probe.swift -L "target/$rust_target/release" -lt3_apple_webtransport_probe \
    -framework Security -framework SystemConfiguration -framework CoreFoundation \
    -o "target/AppleWebTransportProbe-$platform"
  file "target/AppleWebTransportProbe-$platform"
  xcrun vtool -show-build "target/AppleWebTransportProbe-$platform"
done
if [[ "${T3_PROBE_PACKAGE_XCFRAMEWORK:-0}" == 1 ]]; then
  mkdir -p target/include
  cp Probe.h target/include/
  xcodebuild -create-xcframework \
    -library target/aarch64-apple-ios/release/libt3_apple_webtransport_probe.a -headers target/include \
    -library target/aarch64-apple-ios-sim/release/libt3_apple_webtransport_probe.a -headers target/include \
    -output target/AppleWebTransportProbe.xcframework
fi
