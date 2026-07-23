import argparse
import asyncio
import logging
import signal
import socket
import sys
from contextlib import asynccontextmanager

from pylsl import StreamInfo, StreamOutlet, local_clock  # type: ignore

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
    def __init__(self, content_type, nominal_srate, max_queue_size):
        self.content_type = content_type
        self.nominal_srate = nominal_srate
        self.queue = asyncio.Queue(maxsize=max_queue_size)

        self.outlets = {}
        self._lock = asyncio.Lock()

    async def get_outlet(self, header):
        avatar_index = header["avatar_index"]

        if avatar_index in self.outlets:
            return self.outlets[avatar_index]

        logger.debug(header)

        # Offload blocking creation to a thread to keep the event loop free to
        # handle incoming UDP packets.
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
                    # Sentinel signal to terminate.
                    self.queue.task_done()
                    break

                # Unpack item.
                time_stamp, data = item

                if not valid_packet(data):
                    logger.debug(f"Invalid packet length: {len(data)}")
                    self.queue.task_done() # Keep the queue count accurate.
                    continue               # Process next packet.

                header, sample = parse_data(data)

                outlet = await self.get_outlet(header)

                outlet.push_chunk(sample, time_stamp)
                self.queue.task_done()
        finally:
            # Cleanup outlets.
            self.outlets.clear()


@asynccontextmanager
async def udp_lsl_relay(
    local_addr, lsl_writer: LSLWriter, task_group: asyncio.TaskGroup
):
    loop = asyncio.get_running_loop()

    worker_task = task_group.create_task(lsl_writer.lsl_worker())

    transport, protocol = await loop.create_datagram_endpoint(
        lambda: DatagramReader(lsl_writer.queue),
        local_addr=local_addr,
    )

    try:
        yield protocol
    finally:
        # Stop UDP reader.
        transport.close()

        # Stop LSL writer.
        lsl_writer.queue.put_nowait(None)
        if not worker_task.done():
            try:
                await worker_task
            except asyncio.CancelledError:
                pass
        logger.info("\nRelay stopped.")


def setup_signals(stop_event: asyncio.Event):
    loop = asyncio.get_running_loop()

    if sys.platform != "win32":
        for sig in (signal.SIGINT, signal.SIGTERM):
            loop.add_signal_handler(sig, stop_event.set)
    else:
        # Windows fallback.
        signal.signal(
            signal.SIGINT,
            lambda sig, frame: loop.call_soon_threadsafe(stop_event.set),
        )


async def main_task(local_addr, content_type, nominal_srate, max_queue_size):
    stop_event = asyncio.Event()
    setup_signals(stop_event)

    lsl_writer = LSLWriter(content_type, nominal_srate, max_queue_size)

    async with asyncio.TaskGroup() as tg, udp_lsl_relay(local_addr, lsl_writer, tg):
        print("Relay is active. Press Ctrl-c to stop.")
        await stop_event.wait()


def main():
    parser = argparse.ArgumentParser(
        description="""Create an LSL stream for broadcast Perception Neuron
        data.""",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--ip", default="0.0.0.0", help="PN host IP address.")
    parser.add_argument("--port", type=int, default=7002, help="PN host port.")
    parser.add_argument("--content-type", default="mocap", help="Stream content type.")
    parser.add_argument(
        "--nominal-srate",
        type=int,
        default=125,
        help="Stream nominal sample rate.",
    )
    parser.add_argument(
        "--udp-queue-max",
        type=int,
        default=2048,
        help="Number of incoming UDP packets to buffer.",
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
                args.udp_queue_max,
            )
        )
