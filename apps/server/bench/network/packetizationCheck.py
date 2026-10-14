"""Checks receiver behavior for ten datagrams submitted as one UDP GSO batch."""
import json
import socket
import struct
import sys
import time

mode = sys.argv[1]
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
    for _ in range(7 if mode == "drop" else 10):
        packet = receiver.recv(65535)
        assert len(packet) == 1200
        received.append(packet[0])
        times.append(round((time.monotonic() - started) * 1000, 1))
    if mode == "drop":
        assert received == [1, 2, 4, 5, 7, 8, 10], received
        receiver.setblocking(False)
        try:
            receiver.recv(65535)
            raise AssertionError("Unexpected surviving datagram")
        except BlockingIOError:
            pass
    else:
        assert received[:7] == [1, 2, 4, 5, 7, 8, 10], received
        assert received[7:] == [3, 6, 9], received
    print(json.dumps({"mode": mode, "received": received, "arrivalMs": times}))
