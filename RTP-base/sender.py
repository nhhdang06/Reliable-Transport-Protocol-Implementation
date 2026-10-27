"""RTP-base sender: UDP + go-back-N with cumulative acknowledgments."""

import argparse
import socket
import sys
import time

from utils import PacketHeader, compute_checksum

START, END, DATA, ACK = 0, 1, 2, 3
MAX_PAYLOAD = 1456  # Ethernet MTU 1500 - IPv4 20 - UDP 8 - RTP 16
TIMEOUT = 0.5


def make_packet(packet_type, sequence, payload=b""):
    """Attach the supplied RTP header and CRC32 checksum to raw bytes."""
    header = PacketHeader(type=packet_type, seq_num=sequence, length=len(payload))
    header.checksum = compute_checksum(header / payload)
    return bytes(header / payload)


def read_ack(raw):
    """Return an ACK number, or None for a malformed/corrupted packet."""
    if len(raw) != 16:
        return None
    header = PacketHeader(raw)
    if header.type != ACK or header.length != 0:
        return None
    received_checksum = header.checksum
    header.checksum = 0
    if received_checksum != compute_checksum(header / b""):
        return None
    return header.seq_num


def send_start(sock, destination):
    """Exchange START (seq=0) and ACK (seq=1)."""
    sock.sendto(make_packet(START, 0), destination)
    while True:
        try:
            raw, source = sock.recvfrom(2048)
        except socket.timeout:
            # The assignment does not require loss recovery for START.
            # A bounded recv timeout here makes it safe to re-check state.
            continue
        if source == destination and read_ack(raw) == 1:
            return


def send_end(sock, destination, sequence):
    """Wait at most 500 ms for the END ACK, as required."""
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
        ack = read_ack(raw)
        if source == destination and ack == sequence + 1:
            return


def sender(receiver_ip, receiver_port, window_size):
    if window_size < 1:
        raise ValueError("window_size must be at least 1")

    destination = (socket.gethostbyname(receiver_ip), receiver_port)
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.settimeout(TIMEOUT)
        send_start(sock, destination)

        base = 1                  # First packet not cumulatively acknowledged
        next_sequence = 1         # Sequence number for the next new DATA packet
        pending = {}              # sequence -> already-encoded UDP payload
        finished_reading = False
        deadline = None

        while not finished_reading or pending:
            # Stream stdin instead of storing the whole input file in memory.
            while not finished_reading and next_sequence < base + window_size:
                chunk = sys.stdin.buffer.read(MAX_PAYLOAD)
                if not chunk:
                    finished_reading = True
                    break
                packet = make_packet(DATA, next_sequence, chunk)
                pending[next_sequence] = packet
                sock.sendto(packet, destination)
                next_sequence += 1
                if deadline is None:
                    deadline = time.monotonic() + TIMEOUT

            if not pending:
                continue

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                # Go-Back-N: resend all packets still in the window.
                for packet in pending.values():
                    sock.sendto(packet, destination)
                deadline = time.monotonic() + TIMEOUT
                continue

            sock.settimeout(remaining)
            try:
                raw, source = sock.recvfrom(2048)
            except socket.timeout:
                continue

            ack = read_ack(raw) if source == destination else None
            if ack is None or not base < ack <= next_sequence:
                continue  # Ignore duplicate, old, future, and invalid ACKs.

            for number in range(base, ack):
                pending.pop(number, None)
            base = ack
            deadline = time.monotonic() + TIMEOUT if pending else None

        send_end(sock, destination, next_sequence)


def main():
    parser = argparse.ArgumentParser(description="Reliable RTP-base UDP sender")
    parser.add_argument("receiver_ip")
    parser.add_argument("receiver_port", type=int)
    parser.add_argument("window_size", type=int)
    args = parser.parse_args()
    sender(args.receiver_ip, args.receiver_port, args.window_size)


if __name__ == "__main__":
    main()
