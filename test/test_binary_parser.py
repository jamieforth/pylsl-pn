import os

from pylsl_pn import parser
import numpy as np


def test_verify_header():
    header = bytearray(64)
    # Set start token.
    header[:2] = 0xddff.to_bytes(2, "little")
    # Set end token.
    header[-2:] = 0xeeff.to_bytes(2, "little")
    assert parser.verify_header(header)


def test_parse_header():
    with open("test/data/sample-data-1-packet.dat", mode="rb") as file:
        # Read header.
        data = file.read(64)
        header = parser.parse_header(data)
        assert header["protocol"] == "1.1.0.0"
        assert header["count"] == 354
        assert header["with_disp"] is True
        assert header["with_ref"] is False
        assert header["avatar_index"] == 0
        assert header["avatar_name"] == "RED"
        assert header["frame_index"] == 54
        assert header["reserved_0"] == 1
        assert header["reserved_1"] == 0
        assert header["reserved_2"] == 0


def test_parse_data():
    with open("test/data/sample-data-2-packets.dat", mode="rb") as file:
        # Read first header.
        data = file.read(64)
        count = parser.parse_header(data)["count"]

        # Re-read as entire datagram.
        file.seek(-64, os.SEEK_CUR)
        data = file.read(64 + count * 4)
        header, motion = parser.parse_data(data)
        assert header["protocol"] == "1.1.0.0"
        assert header["count"] == 354
        assert header["with_disp"] is True
        assert header["with_ref"] is False
        assert header["avatar_index"] == 0
        assert header["avatar_name"] == "RED"
        assert header["frame_index"] == 54
        assert header["reserved_0"] == 1
        assert header["reserved_1"] == 0
        assert header["reserved_2"] == 0

        np.testing.assert_equal(motion[0], -55.208133697509766)
        np.testing.assert_equal(motion[-1], -20)

        # Read second header.
        data = file.read(64)
        count = parser.parse_header(data)["count"]

        # Re-read as entire datagram.
        file.seek(-64, os.SEEK_CUR)
        data = file.read(64 + count * 4)
        header, motion = parser.parse_data(data)
        assert header["protocol"] == "1.1.0.0"
        assert header["count"] == 354
        assert header["with_disp"] is True
        assert header["with_ref"] is False
        assert header["avatar_index"] == 1
        assert header["avatar_name"] == "BLUE"
        assert header["frame_index"] == 55
        assert header["reserved_0"] == 1
        assert header["reserved_1"] == 0
        assert header["reserved_2"] == 0

        np.testing.assert_equal(motion[0], -55.21929168701172)
        np.testing.assert_equal(motion[-1], -20)
