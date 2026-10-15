#!/usr/bin/env python3
"""Run the caller's native probe against a loopback-only, pinned Node WebTransport fixture."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import selectors
import subprocess
import tempfile

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--node-modules", required=True, type=Path, help="directory containing package.json and node_modules")
parser.add_argument("--probe", required=True, type=Path, help="compiled Rust or Swift executable")
args = parser.parse_args()
scratch_root = Path.home() / "codex-tmp"
scratch_root.mkdir(exist_ok=True)
with tempfile.TemporaryDirectory(prefix="t3-apple-quic-", dir=scratch_root) as scratch:
    scratch = Path(scratch)
    cert, key = scratch / "cert.pem", scratch / "key.pem"
    subprocess.run([
        "openssl", "req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:prime256v1",
        "-sha256", "-nodes", "-days", "2", "-subj", "/CN=localhost",
        "-keyout", str(key), "-out", str(cert),
    ], check=True, capture_output=True)
    der = subprocess.check_output(["openssl", "x509", "-in", str(cert), "-outform", "DER"])
    fingerprint = ":".join(f"{value:02x}" for value in hashlib.sha256(der).digest())
    env = dict(os.environ, T3_PROBE_NODE_MODULES=str(args.node_modules.resolve()))
    server = subprocess.Popen(
        ["node", str(Path(__file__).with_name("fixture.mjs")), str(cert), str(key)],
        env=env, stdout=subprocess.PIPE, text=True,
    )
    try:
        selector = selectors.DefaultSelector()
        selector.register(server.stdout, selectors.EVENT_READ)
        if not selector.select(20):
            raise TimeoutError("fixture did not publish its loopback endpoint")
        endpoint = json.loads(server.stdout.readline())["url"]
        print(f"fixture={endpoint}", flush=True)
        subprocess.run([str(args.probe.resolve()), endpoint, fingerprint], check=True, timeout=75)
    finally:
        server.terminate()
        try:
            server.wait(timeout=5)
        except subprocess.TimeoutExpired:
            server.kill()
            server.wait(timeout=5)
        print(f"fixture exited={server.returncode}", flush=True)
