import argparse
import asyncio
import logging
import signal
import socket
import sys
from contextlib import asynccontextmanager

from pylsl import IRREGULAR_RATE, StreamInfo, StreamOutlet, local_clock  # type: ignore

from .parser import parse_data, valid_packet

logger = logging.getLogger(__name__)


class DatagramReader(asyncio.DatagramProtocol):
    def __init__(self, queue):
        self.queue = queue
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport

        logger.debug(
            "Socket buffer size: %d bytes",
            transport.get_extra_info("socket").getsockopt(
                socket.SOL_SOCKET, socket.SO_RCVBUF
            ),
        )

    def datagram_received(self, data: bytes, addr):
        # Timestamp immediately once OS has written data to the buffer. This is
        # as accurate as we can get without relying on lower-level network
        # timestamps, which are platform specific and would require additional
        # synchronisation with the LSL clock. As such these timestamps will
        # contain some jitter, but it's probably acceptable for most cases (< 1
        # millisecond).
        time_stamp = local_clock()

        try:
            self.queue.put_nowait((time_stamp, data))
        except asyncio.QueueFull:
            logger.warning("UDP queue full – dropping packet")


class LSLWriter():
    def __init__(self, queue, content_type, nominal_srate):
        self.queue = queue
        self.content_type = content_type
        self.nominal_srate = nominal_srate

        self.outlets = {}
        self._lock = asyncio.Lock()

    async def get_outlet(self, header):
        avatar_index = header["avatar_index"]

        if avatar_index in self.outlets:
            return self.outlets[avatar_index]

        async with self._lock:
            # Double-check pattern to avoid race conditions.
            if avatar_index not in self.outlets:
                # Offload blocking creation to a thread.
                outlet = await asyncio.to_thread(
                    self._create_lsl_stream,
                    header["avatar_name"],
                    header["count"],
                )
                self.outlets[avatar_index] = outlet

            return self.outlets[avatar_index]

    def _create_lsl_stream(self, avatar_name, channel_count):
        logger.info(f"Creating LSL outlet: {avatar_name} ({channel_count} channels)")
        info = StreamInfo(
            f"Avatar: {avatar_name}",
            self.content_type,
            channel_count,
            self.nominal_srate,
            "float32",
        )
        outlet = StreamOutlet(info)
        return outlet

    async def lsl_worker(self):
        try:
            while True:
                item = await self.queue.get()
                if item is None:
                    self.queue.task_done()
                    break

                # Unpack item.
                time_stamp, data = item

                if not valid_packet(data):
                    logger.debug(f"Invalid packet length: {len(data)}")
                    return

                header, sample = parse_data(data)
                # logger.debug(header)

                outlet = await self.get_outlet(header)

                # Offload the LSL call to a thread to keep the event loop free to
                # handle incoming UDP packets.
                await asyncio.to_thread(outlet.push_chunk, sample, time_stamp)

                self.queue.task_done()
        finally:
            # Cleanup outlets.
            self.outlets.clear()


@asynccontextmanager
async def udp_lsl_relay(local_addr, content_type, nominal_srate):
    loop = asyncio.get_running_loop()
    queue = asyncio.Queue(maxsize=2048)
    lsl_writer = LSLWriter(queue, content_type, nominal_srate)
    worker_task = asyncio.create_task(lsl_writer.lsl_worker())
    transport, protocol = await loop.create_datagram_endpoint(
        lambda: DatagramReader(queue),
        local_addr=local_addr,
    )
    try:
        yield protocol
    finally:
        # Stop UDP reader.
        transport.close()

        # Stop LSL writer.
        queue.put_nowait(None)
        await worker_task

        logger.info("\nRelay stopped.")


async def main_task(local_addr, content_type, nominal_srate):
    loop = asyncio.get_running_loop()
    stop_event = asyncio.Event()

    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop_event.set)

    async with udp_lsl_relay(local_addr, content_type, nominal_srate):
        print("Relay is active. Press Ctrl-c to stop.")
        await stop_event.wait()


def main():
    parser = argparse.ArgumentParser(
        description="""Create an LSL stream for broadcast Perception Neuron
        data.""",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--ip", default="127.0.0.1", help="PN host IP address.")
    parser.add_argument("--port", type=int, default=7005, help="PN host port.")
    parser.add_argument("--content_type", default="misc", help="Stream content type.")
    parser.add_argument(
        "--nominal_srate",
        type=int,
        default=IRREGULAR_RATE,
        help="Stream nominal sample rate.",
    )
    parser.add_argument(
        "--verbose", action="store_true", help="Print extra debugging information."
    )
    args = parser.parse_args()

    log_level = logging.INFO
    if args.verbose:
        log_level = logging.DEBUG
    logging.basicConfig(level=log_level)

    loop_factory = None
    if sys.platform != "win32":
        try:
            import uvloop

            loop_factory = uvloop.new_event_loop
        except ImportError:
            pass

    with asyncio.Runner(loop_factory=loop_factory) as runner:
        # Block until runner returns.
        runner.run(
            main_task(
                (args.ip, args.port),
                args.content_type,
                args.nominal_srate,
            )
        )
