#!/usr/bin/env bash
set -euo pipefail
export TMPDIR="$HOME/codex-tmp/t3-apple-quic-build-tmp"
mkdir -p "$TMPDIR"
scratch=$(mktemp -d "$TMPDIR/public-api.XXXXXX")
trap 'rm -rf "$scratch"' EXIT
sdk=$(xcrun --sdk iphoneos --show-sdk-path)
cat > "$scratch/raw-quic.swift" <<'SWIFT'
import Network
func publicQUIC() -> NWParameters {
    NWParameters(quic: NWProtocolQUIC.Options(alpn: ["h3"]))
}
SWIFT
xcrun --sdk iphoneos swiftc -target arm64-apple-ios18.0 -sdk "$sdk" \
  -typecheck "$scratch/raw-quic.swift"
echo "PASS public raw QUIC API typechecks for iOS 18.0"
cat > "$scratch/webtransport.c" <<'C'
#include <Network/Network.h>
void native_webtransport(void) {
    (void)nw_parameters_create_webtransport_http;
    (void)nw_webtransport_create_options;
}
C
if xcrun --sdk iphoneos clang -target arm64-apple-ios18.0 -isysroot "$sdk" \
  -fsyntax-only -fblocks "$scratch/webtransport.c"; then
  echo "Public SDK now exposes WebTransport. Re-evaluate the native Apple implementation."
  exit 1
fi
echo "EXPECTED public WebTransport API compile failure; symbols in Network.tbd are insufficient"
if rg -n -i webtransport "$sdk/System/Library/Frameworks/Network.framework/Headers" \
  "$sdk/System/Library/Frameworks/Network.framework/Modules"; then
  echo "Public declarations found. Inspect SDK availability before relying on this result."
  exit 1
fi
