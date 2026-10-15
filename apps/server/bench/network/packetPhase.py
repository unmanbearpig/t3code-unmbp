"""Advance a periodic ingress counter, draining all priming datagrams before setup."""
import socket
import sys

count = int(sys.argv[1])
with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as receiver, socket.socket(
    socket.AF_INET, socket.SOCK_DGRAM
) as sender:
    receiver.bind(("127.0.0.1", 5679))
    receiver.settimeout(3)
    # Bound the receive queue even for offsets greater than a byte can represent.
    for start in range(0, count, 64):
        pending = set(range(start, min(start + 64, count)))
        for index in pending:
            sender.sendto(index.to_bytes(8, "big"), receiver.getsockname())
        while pending:
            index = int.from_bytes(receiver.recv(65535), "big")
            assert index in pending, "unexpected or duplicate priming datagram"
            pending.remove(index)
