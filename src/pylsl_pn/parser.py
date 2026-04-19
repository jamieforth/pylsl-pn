import struct

import numpy as np

header_token_format = ">H"
header_start_token = 0xffdd
header_end_token = 0xffee
header_format = "<IHHHH32sII8s"


def verify_header(data):
    start_token = struct.unpack_from(header_token_format, data)[0]
    end_token = struct.unpack_from(header_token_format, data, 62)[0]
    return (start_token == header_start_token) & (end_token == header_end_token)


def valid_packet(data):
    if len(data) < 64:
        return False
    return verify_header(data)


def parse_header(data):
    header = struct.unpack_from(header_format, data, 2)
    header_fields = {
        "protocol": header[0],
        "count": header[1],
        "with_disp": header[2],
        "with_ref": header[3],
        "avatar_index": header[4],
        "avatar_name": header[5].split(b"\x00")[0].decode("utf-8"),
        "frame_index": header[6],
        "data_type": header[7],  # Rotation order?
        # Reserved block: header[9],
    }
    return header_fields


def parse_motion(data, count):
    #return struct.unpack_from(f"<{count}f", data, 64)
    return np.frombuffer(data, np.float32, count, 64)

def parse_data(data, buffer=None):
    header = parse_header(data)
    motion = parse_motion(data, header["count"])
    return header, motion
