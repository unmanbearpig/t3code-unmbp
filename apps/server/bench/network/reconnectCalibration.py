"""Numbered UDP echoes and directional TCP bulk. No forwarding hop."""
import json
import select
import socket
import sys
import threading
import time

PORT = 4410


def serve():
    udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp.bind(('127.0.0.1', PORT))
    tcp = socket.socket()
    tcp.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    tcp.bind(('127.0.0.1', PORT))
    tcp.listen()

    def echo():
        while True:
            data, address = udp.recvfrom(2048)
            udp.sendto(data, address)

    def bulk_connection(connection):
        with connection:
            mode = connection.recv(1)
            if mode == b'D':
                size = b''
                while len(size) < 8:
                    size += connection.recv(8 - len(size))
                remaining = int.from_bytes(size, 'big')
                block = b'x' * 65536
                while remaining:
                    data = block[:remaining]
                    connection.sendall(data)
                    remaining -= len(data)
            else:
                while connection.recv(65536):
                    pass
                connection.sendall(b'K')

    threading.Thread(target=echo, daemon=True).start()
    print(json.dumps({'type': 'ready'}), flush=True)
    while True:
        connection, _ = tcp.accept()
        threading.Thread(target=bulk_connection, args=(connection,), daemon=True).start()


def echo_client(duration_ms):
    udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp.setblocking(False)
    udp.connect(('127.0.0.1', PORT))
    print(json.dumps({'type': 'ready'}), flush=True)
    signal = json.loads(sys.stdin.readline())
    start = int(signal['startNs'])
    next_send = start
    end_send = start + duration_ms * 1_000_000
    deadline = end_send + 3_000_000_000
    number = 0
    received = []
    sent = {}
    while time.monotonic_ns() < deadline:
        now = time.monotonic_ns()
        if now >= next_send and now < end_send:
            payload = number.to_bytes(4, 'big') + b'x' * 96
            sent[number] = now
            udp.send(payload)
            number += 1
            next_send += 20_000_000
        timeout = max(0, min(0.02, (next_send - time.monotonic_ns()) / 1e9)) if now < end_send else 0.05
        if select.select([udp], [], [], timeout)[0]:
            packet = udp.recv(2048)
            index = int.from_bytes(packet[:4], 'big')
            arrived = time.monotonic_ns()
            received.append({'number': index, 'sentMs': (sent[index] - start) / 1e6,
                             'receivedMs': (arrived - start) / 1e6,
                             'rttMs': (arrived - sent[index]) / 1e6})
        if now >= end_send and len(received) == number:
            break
    print(json.dumps({'type': 'result', 'sent': number, 'echoes': received}), flush=True)


def bulk_client(direction, size):
    client = socket.socket()
    client.settimeout(60)
    client.connect(('127.0.0.1', PORT))
    start = time.monotonic_ns()
    count = 0
    if direction == 'down':
        client.sendall(b'D' + size.to_bytes(8, 'big'))
        while True:
            data = client.recv(65536)
            if not data:
                break
            count += len(data)
    else:
        client.sendall(b'U')
        data = b'x' * size
        client.sendall(data)
        client.shutdown(socket.SHUT_WR)
        if client.recv(1) != b'K':
            raise RuntimeError('Upload not acknowledged')
        count = len(data)
    elapsed = (time.monotonic_ns() - start) / 1e9
    client.close()
    print(json.dumps({'type': 'result', 'direction': direction, 'bytes': count,
                      'seconds': elapsed, 'kbitPerSecond': count * 8 / elapsed / 1000}), flush=True)


if __name__ == '__main__':
    if sys.argv[1] == 'server':
        serve()
    elif sys.argv[1] == 'echo':
        echo_client(int(sys.argv[2]))
    else:
        bulk_client(sys.argv[2], int(sys.argv[3]))
