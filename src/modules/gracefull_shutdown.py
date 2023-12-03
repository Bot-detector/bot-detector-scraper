import asyncio
import logging
import signal

logger = logging.getLogger(__name__)


class GracefulShutdown:
    def __init__(self, shutdown_event: asyncio.Event, shutdown_sequence):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)
        self.shutdown_event = shutdown_event
        self.shutdown_sequence = shutdown_sequence

    def exit_gracefully(self, signum, frame):
        logger.info("shutdown")
        self.shutdown_event.set()

        loop = asyncio.get_event_loop()
        loop.create_task(self.shutdown_sequence)
