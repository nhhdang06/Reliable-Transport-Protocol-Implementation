"""RTP-opt sender: selectively retransmit unacknowledged packets."""

import argparse
import socket
import sys
import time

from utils import PacketHeader, compute_checksum

START, END, DATA, ACK = 0, 1, 2, 3
MAX_PAYLOAD = 1456
TIMEOUT = 0.5


def make_packet(packet_type, sequence, payload=b""):
    header = PacketHeader(type=packet_type, seq_num=sequence, length=len(payload))
    header.checksum = compute_checksum(header / payload)
    return bytes(header / payload)


def read_ack(raw):
    if len(raw) != 16:
        return None
    header = PacketHeader(raw)
    if header.type != ACK or header.length != 0:
        return None
    checksum = header.checksum
    header.checksum = 0
    if checksum != compute_checksum(header / b""):
        return None
    return header.seq_num


def send_start(sock, destination):
    sock.sendto(make_packet(START, 0), destination)
    while True:
        try:
            raw, source = sock.recvfrom(2048)
        except socket.timeout:
            continue
        if source == destination and read_ack(raw) == 1:
            return


def send_end(sock, destination, sequence):
    sock.sendto(make_packet(END, sequence), destination)
    deadline = time.monotonic() + TIMEOUT
    while True:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            return
        sock.settimeout(remaining)
        try:
            raw, source = sock.recvfrom(2048)
        except socket.timeout:
            return
        if source == destination and read_ack(raw) == sequence + 1:
            return


def sender(receiver_ip, receiver_port, window_size):
    if window_size < 1:
        raise ValueError("window_size must be at least 1")

    destination = (socket.gethostbyname(receiver_ip), receiver_port)
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.settimeout(TIMEOUT)
        send_start(sock, destination)

        base = 1
        next_sequence = 1
        in_flight = {}
        finished_reading = False
        deadline = None

        while not finished_reading or in_flight:
            while not finished_reading and next_sequence < base + window_size:
                chunk = sys.stdin.buffer.read(MAX_PAYLOAD)
                if not chunk:
                    finished_reading = True
                    break
                packet = make_packet(DATA, next_sequence, chunk)
                in_flight[next_sequence] = [packet, False]
                sock.sendto(packet, destination)
                next_sequence += 1
                if deadline is None:
                    deadline = time.monotonic() + TIMEOUT

            if not in_flight:
                continue

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                for packet, acknowledged in in_flight.values():
                    if not acknowledged:
                        sock.sendto(packet, destination)
                deadline = time.monotonic() + TIMEOUT
                continue

            sock.settimeout(remaining)
            try:
                raw, source = sock.recvfrom(2048)
            except socket.timeout:
                continue

            ack = read_ack(raw) if source == destination else None
            if ack is None or ack not in in_flight:
                continue

            in_flight[ack][1] = True
            old_base = base
            while base in in_flight and in_flight[base][1]:
                del in_flight[base]
                base += 1
            if base != old_base:
                deadline = time.monotonic() + TIMEOUT if in_flight else None

        send_end(sock, destination, next_sequence)


def main():
    parser = argparse.ArgumentParser(description="Reliable RTP-opt UDP sender")
    parser.add_argument("receiver_ip")
    parser.add_argument("receiver_port", type=int)
    parser.add_argument("window_size", type=int)
    args = parser.parse_args()
    sender(args.receiver_ip, args.receiver_port, args.window_size)


if __name__ == "__main__":
    main()
