#!/usr/bin/env python3
"""Run host/JNI interoperability against the pinned Node backend, with verified TLS."""
import argparse
import json
import os
from pathlib import Path
import socket
import subprocess
import threading

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--evidence", type=Path, required=True)
parser.add_argument("--node-modules", type=Path, required=True)
args = parser.parse_args()
root = Path(__file__).resolve().parents[1]
evidence = args.evidence.resolve()
evidence.mkdir(parents=True, exist_ok=True)
commands = []
env = os.environ | {"T3_QUIC_PROBE_TOKEN": "isolated-fixture-not-a-user-token"}


def run(command, name, *, expected=0, custom_env=None):
    commands.append(command)
    result = subprocess.run(command, cwd=root, env=custom_env or env,
                            text=True, capture_output=True, timeout=40)
    (evidence / f"{name}.stdout").write_text(result.stdout)
    (evidence / f"{name}.stderr").write_text(result.stderr)
    if result.returncode != expected:
        raise RuntimeError(f"{name} exited {result.returncode}, expected {expected}: {result.stderr} {result.stdout}")
    return result


ca_key = evidence / "fixture-ca.key"
ca = evidence / "fixture-ca.pem"
key = evidence / "fixture-server.key"
cert = evidence / "fixture-server.pem"
wrong_ca = evidence / "wrong-ca.pem"
wrong_key = evidence / "wrong-ca.key"
ext = evidence / "fixture-extensions.cnf"
ext.write_text("basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature\n"
               "extendedKeyUsage=serverAuth\nsubjectAltName=IP:127.0.0.1,DNS:localhost\n")
run(["openssl", "req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256",
     "-nodes", "-keyout", str(ca_key), "-out", str(ca), "-days", "2", "-subj", "/CN=T3 isolated CA",
     "-addext", "basicConstraints=critical,CA:TRUE"], "generate-ca")
run(["openssl", "req", "-new", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256", "-nodes",
     "-keyout", str(key), "-out", str(evidence / "fixture.csr"), "-subj", "/CN=localhost"], "generate-leaf")
run(["openssl", "x509", "-req", "-in", str(evidence / "fixture.csr"), "-CA", str(ca), "-CAkey", str(ca_key),
     "-set_serial", "1", "-out", str(cert), "-days", "2", "-extfile", str(ext)], "sign-leaf")
run(["openssl", "req", "-x509", "-newkey", "ec", "-pkeyopt", "ec_paramgen_curve:P-256", "-nodes",
     "-keyout", str(wrong_key), "-out", str(wrong_ca), "-days", "2", "-subj", "/CN=Wrong isolated CA",
     "-addext", "basicConstraints=critical,CA:TRUE"], "generate-wrong-ca")
run(["openssl", "verify", "-CAfile", str(ca), "-verify_ip", "127.0.0.1", str(cert)], "verify-leaf")

# Reserve a loopback UDP port briefly. Every owned process is tracked and reaped below.
with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as reservation:
    reservation.bind(("127.0.0.1", 0))
    port = reservation.getsockname()[1]
server_command = ["node", str(root / "fixture/server.mjs"), str(args.node_modules.resolve()),
                  str(cert), str(key), str(port)]
commands.append(server_command)
server = subprocess.Popen(server_command, cwd=root, env=env, stdout=subprocess.PIPE,
                          stderr=subprocess.STDOUT, text=True, bufsize=1)
events = []
ready = threading.Event()


def drain():
    with (evidence / "server.log").open("w") as output:
        for line in server.stdout:
            output.write(line)
            output.flush()
            try:
                event = json.loads(line)
                events.append(event)
                if event.get("event") == "ready":
                    ready.set()
            except json.JSONDecodeError:
                pass


reader = threading.Thread(target=drain)
reader.start()
summary = {"device_execution": False, "server_pid": server.pid, "scenarios": []}
try:
    if not ready.wait(15):
        raise RuntimeError("fixture server did not become ready; see server.log")
    url = f"https://127.0.0.1:{port}/probe"
    binary = str(root / "target/debug/quic-probe")
    success = run([binary, url, str(ca), "15000"], "host-success")
    report = json.loads(success.stdout)
    if not report["ok"] or len(report["checks"]) != 5:
        raise RuntimeError(f"missing probe checks: {report}")
    summary["scenarios"].append({"name": "host-verified-tls-streams-reset-timeout-close", "result": report})

    untrusted = run([binary, url, str(wrong_ca), "5000"], "host-untrusted-ca", expected=1)
    rejection = json.loads(untrusted.stdout)
    if rejection["ok"] or "UnknownIssuer" not in rejection["error"]:
        raise RuntimeError(f"untrusted CA did not reject at TLS: {rejection}")
    summary["scenarios"].append({"name": "untrusted-ca-rejected", "result": rejection})

    unauthorized = run([binary, url, str(ca), "5000"], "host-bad-auth", expected=1,
                       custom_env=env | {"T3_QUIC_PROBE_TOKEN": "incorrect-fixture-token"})
    rejection = json.loads(unauthorized.stdout)
    if rejection["ok"] or "server rejected WebTransport session request" not in rejection["error"]:
        raise RuntimeError(f"invalid token did not reject CONNECT: {rejection}")
    summary["scenarios"].append({"name": "invalid-auth-rejected", "result": rejection})

    # No listener on this newly reserved port. The application deadline must bound the attempt.
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as unavailable:
        unavailable.bind(("127.0.0.1", 0))
        absent = f"https://127.0.0.1:{unavailable.getsockname()[1]}/probe"
        deadline = run([binary, absent, str(ca), "500"], "host-connect-deadline", expected=1)
    rejection = json.loads(deadline.stdout)
    if rejection["ok"] or "deadline exceeded" not in rejection["error"] or rejection["elapsed_ms"] > 3000:
        raise RuntimeError(f"unreachable endpoint did not obey deadline: {rejection}")
    summary["scenarios"].append({"name": "connect-deadline", "result": rejection})

    classes = root / "build/host-classes"
    classes.mkdir(parents=True, exist_ok=True)
    run(["javac", "--release", "8", "-d", str(classes),
         str(root / "java/dev/t3/quicprobe/NativeProbe.java"),
         str(root / "java/dev/t3/quicprobe/ProbeMain.java")], "javac-host-jni")
    jni = run(["java", f"-Djava.library.path={root / 'target/debug'}", "-cp", str(classes),
               "dev.t3.quicprobe.ProbeMain", url, str(ca)], "host-jni-success")
    report = json.loads(jni.stdout)
    if not report["ok"]:
        raise RuntimeError(f"JNI entrypoint failed: {report}")
    summary["scenarios"].append({"name": "host-jvm-exact-jni-entrypoint", "result": report})
finally:
    server.terminate()
    try:
        server.wait(timeout=5)
    except subprocess.TimeoutExpired:
        server.kill()
        server.wait(timeout=5)
    reader.join(timeout=5)
    summary["server_exit_code"] = server.returncode
    summary["server_events"] = events
    summary["server_reaped"] = server.poll() is not None and not reader.is_alive()
    (evidence / "commands.json").write_text(json.dumps(commands, indent=2) + "\n")
    (evidence / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    for path in [ca_key, key, wrong_key, evidence / "fixture.csr"]:
        path.unlink(missing_ok=True)

for op, expected_code in [(1, 42), (2, 44)]:
    if not any(event.get("event") == "receive-reset" and event.get("op") == op and
               event.get("streamErrorCode") == expected_code for event in events):
        raise RuntimeError(f"server did not observe reset {expected_code} for stream operation {op}")
    if not any(event.get("event") == "send-stopped" and event.get("op") == op and
               event.get("streamErrorCode") == expected_code + 1 for event in events):
        raise RuntimeError(f"server did not observe STOP_SENDING {expected_code + 1} for operation {op}")
if not any(event.get("event") == "connect" and event.get("authenticated") and event.get("nativeHeader")
           for event in events):
    raise RuntimeError("server did not confirm authenticated custom-header CONNECT")
summary["ok"] = True
(evidence / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
print(json.dumps({"ok": True, "scenarios": len(summary["scenarios"]), "server_reaped": summary["server_reaped"]}))
