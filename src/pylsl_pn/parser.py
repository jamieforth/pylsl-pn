import struct

import numpy as np

header_token_format = "<H"
header_start_token = 0xddff
header_end_token = 0xeeff
protocol_format = "<BBBB"
header_format = "<H??I32sIIII"


def valid_packet(data):
    if len(data) < 64:
        return False
    return verify_header(data)


def verify_header(data):
    start_token = struct.unpack_from(header_token_format, data)[0]
    end_token = struct.unpack_from(header_token_format, data, 62)[0]
    return (start_token == header_start_token) & (end_token == header_end_token)


def parse_data(data):
    header = parse_header(data)
    motion = parse_motion(data, header["count"])
    return header, motion


def parse_header(data):
    protocol_version = parse_version(data)
    header = struct.unpack_from(header_format, data, 6)
    header_fields = {
        "protocol": protocol_version,
        "count": header[0],
        "with_disp": header[1],
        "with_ref": header[2],
        "avatar_index": header[3],
        "avatar_name": header[4].split(b"\x00")[0].decode("utf-8"),
        "frame_index": header[5],
        "reserved_0": header[6],  # Rotation order?
        "reserved_1": header[7],
        "reserved_2": header[8],
    }
    return header_fields


def parse_version(data):
    build, revision, minor, major = struct.unpack_from(protocol_format, data, 2)
    return f"{major}.{minor}.{revision}.{build}"


def parse_motion(data, count):
    return np.frombuffer(data, np.float32, count, 64)
