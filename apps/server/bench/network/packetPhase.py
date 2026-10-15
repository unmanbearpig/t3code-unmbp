"""Advance a periodic ingress counter, draining all priming datagrams before setup."""
import socket
import sys

count = int(sys.argv[1])
with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as receiver, socket.socket(
    socket.AF_INET, socket.SOCK_DGRAM
) as sender:
    receiver.bind(("127.0.0.1", 5679))
    receiver.settimeout(3)
    for index in range(count):
        sender.sendto(bytes([index]), receiver.getsockname())
    for index in range(count):
        assert receiver.recv(65535) == bytes([index])
