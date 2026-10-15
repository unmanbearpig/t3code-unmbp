import Foundation

// This executable is a link probe on iOS and a runnable interop probe on macOS.
guard CommandLine.arguments.count == 3 else {
    fatalError("usage: AppleWebTransportProbe <url> <certificate-sha256>")
}
let status = CommandLine.arguments[1].withCString { url in
    CommandLine.arguments[2].withCString { hash in t3_apple_webtransport_probe(url, hash) }
}
exit(status)
