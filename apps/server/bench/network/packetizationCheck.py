"""Checks receiver behavior for ten datagrams submitted as one UDP GSO batch."""
import json
import socket
import struct
import sys
import time

mode = sys.argv[1]
phase = int(sys.argv[2]) if len(sys.argv) > 2 else 0
normal = [index for index in range(1, 11) if (index + phase) % 3 != 0]
selected = [index for index in range(1, 11) if (index + phase) % 3 == 0]
with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as receiver, socket.socket(
    socket.AF_INET, socket.SOCK_DGRAM
) as sender:
    receiver.bind(("127.0.0.1", 5678))
    receiver.settimeout(3)
    started = time.monotonic()
    sender.sendmsg(
        [b"".join(bytes([index]) * 1200 for index in range(1, 11))],
        [(socket.IPPROTO_UDP, 103, struct.pack("H", 1200))],
        0,
        ("127.0.0.1", 5678),
    )
    received = []
    times = []
    for _ in range(len(normal) if mode == "drop" else 10):
        packet = receiver.recv(65535)
        assert len(packet) == 1200
        received.append(packet[0])
        times.append(round((time.monotonic() - started) * 1000, 1))
    if mode == "drop":
        assert received == normal, received
        receiver.setblocking(False)
        try:
            receiver.recv(65535)
            raise AssertionError("Unexpected surviving datagram")
        except BlockingIOError:
            pass
    else:
        assert received[:len(normal)] == normal, received
        assert received[len(normal):] == selected, received
    print(json.dumps({"mode": mode, "phase": phase, "received": received, "arrivalMs": times}))
