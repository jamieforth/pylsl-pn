import argparse
import asyncio
import logging
import sys
from asyncio.transports import DatagramTransport

from pylsl import IRREGULAR_RATE, StreamInfo, StreamOutlet, local_clock

from .parser import parse_data, valid_packet

logger = logging.getLogger(__name__)

class LSLRelay(asyncio.DatagramProtocol):
    def __init__(self):
        self.outlets = {}

    def connection_made(self, transport: DatagramTransport):
        self.transport = transport

    def datagram_received(self, data, addr):
        # Timestamp immediately once OS has written data to the buffer. This is
        # as accurate as we can get without relying on lower-level network
        # timestamps, which are platform specific and would require additional
        # synchronisation with the LSL clock. As such these timestamps will
        # contain some jitter, but it's probably acceptable for most cases (< 1
        # millisecond).
        ts = local_clock()

        if not valid_packet(data):
            logger.debug(f"Invalid packet length: {len(data)}")
            return

        header, sample = parse_data(data)
        logger.debug(header)

        avatar_index = header["avatar_index"]

        if avatar_index in self.outlets:
            outlet = self.outlets[avatar_index]
        else:
            outlet = self.create_lsl_stream(header)
            self.outlets[avatar_index] = outlet

        outlet.push_chunk(sample, timestamp=ts)

    def create_lsl_stream(self, header):
        info = StreamInfo(
            f"Avatar: {header['avatar_name']}",
            "mocap",
            header["count"],
            IRREGULAR_RATE,
            "float32",
        )
        outlet = StreamOutlet(info)
        return outlet


async def task(local_addr):
    loop = asyncio.get_running_loop()
    transport, protocol = await loop.create_datagram_endpoint(
        LSLRelay,
        local_addr=local_addr,
    )
    await asyncio.Future()


def main():
    parser = argparse.ArgumentParser(
        description="""Create an LSL stream for broadcast Perception Neuron
        data.""",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--ip", default="127.0.0.1", help="PN host IP address.")
    parser.add_argument("--port", type=int, default=7005, help="PN host port.")
    parser.add_argument(
        "--verbose", action="store_true", help="Print extra debugging information."
    )
    args = parser.parse_args()

    log_level = logging.INFO
    if args.verbose:
        log_level = logging.DEBUG

    logging.basicConfig(level=log_level)

    if sys.platform != "win32":
        import uvloop
        uvloop.install()

    try:
        with asyncio.Runner() as runner:
            # Block until runner returns.
            runner.run(task((args.ip, args.port)))
    except KeyboardInterrupt:
        print("Stopping")
