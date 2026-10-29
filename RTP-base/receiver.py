"""RTP-base receiver: packet buffering and cumulative acknowledgments."""

import argparse
import socket
import sys

from utils import PacketHeader, compute_checksum

START, END, DATA, ACK = 0, 1, 2, 3
HEADER_SIZE = 16
MAX_PAYLOAD = 1456


def make_ack(sequence):
    header = PacketHeader(type=ACK, seq_num=sequence, length=0)
    header.checksum = compute_checksum(header / b"")
    return bytes(header / b"")


def parse_packet(raw):
    """Validate packet boundaries and CRC; return (type, seq, data)."""
    if len(raw) < HEADER_SIZE or len(raw) > HEADER_SIZE + MAX_PAYLOAD:
        return None
    header = PacketHeader(raw[:HEADER_SIZE])
    payload = raw[HEADER_SIZE:]
    if header.type not in (START, END, DATA) or header.length != len(payload):
        return None
    if header.type != DATA and payload:
        return None

    original_checksum = header.checksum
    header.checksum = 0
    if compute_checksum(header / payload) != original_checksum:
        return None
    return header.type, header.seq_num, payload


def receiver(receiver_ip, receiver_port, window_size):
    if window_size < 1:
        raise ValueError("window_size must be at least 1")

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:
        sock.bind((receiver_ip, receiver_port))
        peer = None
        expected = 1             
        buffered = {}            
        output = sys.stdout.buffer

        while True:
            raw, address = sock.recvfrom(2048)
            packet = parse_packet(raw)
            if packet is None:
                continue  
            packet_type, sequence, payload = packet

            if peer is None:
                if packet_type != START or sequence != 0:
                    continue
                peer = address
                sock.sendto(make_ack(1), peer)
                continue

            if address != peer or packet_type == START:
                continue  

            if packet_type == DATA:
                if expected <= sequence < expected + window_size:
                    buffered.setdefault(sequence, payload)
                    while expected in buffered:
                        output.write(buffered.pop(expected))
                        expected += 1
                    output.flush()
                
                sock.sendto(make_ack(expected), peer)

            elif packet_type == END:
                if sequence == expected:
                    sock.sendto(make_ack(expected + 1), peer)
                    output.flush()
                    return
                sock.sendto(make_ack(expected), peer)


def main():
    parser = argparse.ArgumentParser(description="Reliable RTP-base UDP receiver")
    parser.add_argument("receiver_ip")
    parser.add_argument("receiver_port", type=int)
    parser.add_argument("window_size", type=int)
    args = parser.parse_args()
    receiver(args.receiver_ip, args.receiver_port, args.window_size)


if __name__ == "__main__":
    main()
