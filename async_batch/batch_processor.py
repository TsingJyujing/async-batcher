import abc
import asyncio
import logging
import time
from asyncio import Future, Event
from queue import Queue
from threading import Thread, Lock
from typing import Iterable

log = logging.getLogger(__file__)


class DataPack(Future):
    def __init__(self, data):
        super().__init__()
        self.data = data


class BatchProcessor(abc.ABC):
    @abc.abstractmethod
    def get_batch_size(self) -> int: pass

    @abc.abstractmethod
    def _process(self, batch_data: Iterable) -> Iterable: pass

    def process(self, batch_data: Iterable[DataPack]):
        results = self._process([d.data for d in batch_data])
        for result, dp in zip(results, batch_data):
            dp.set_result(result)


class TaskQueue(Thread):
    def __init__(
            self,
            batch_processor: BatchProcessor,
            queue_capacity: int = 0,
            batch_time: float = 0.5
    ):
        super().__init__()
        self.batch_time = batch_time
        self.last_execute_tick = 0
        self.batch_processor = batch_processor
        self.stop = False
        self.queue = Queue(maxsize=queue_capacity)
        self.queue_capacity = queue_capacity
        self.mutex_lock = Lock()
        self._loop = asyncio.new_event_loop()
        self.event: Event = Event(loop=self._loop)

    def _size_overflow(self):
        return self.queue.qsize() >= self.batch_processor.get_batch_size()

    def stop(self):
        self.stop = True

    def async_submit(self, data) -> Future:
        if not self.stop:

            dp = DataPack(data)
            self.queue.put(dp)
            log.debug(f"Appended {data}")
            locked = self.mutex_lock.acquire(blocking=False)
            if locked:
                log.debug("Get lock successfully")
                if self._size_overflow():
                    log.debug("Setting event")
                    self.event.set()
                self.mutex_lock.release()
            # else means failed to get lock,
            log.debug("Return future")
            return dp
        else:
            raise Exception("Stop accept tasks")

    def _process(self) -> int:
        with self.mutex_lock:
            log.debug("Start batch function")
            N = min(
                self.queue.qsize(),
                self.batch_processor.get_batch_size()
            )
            log.debug(f"Plan to get {N} data")
            data_buffer = [self.queue.get() for _ in range(N)]
            buffer_size = len(data_buffer)
            log.debug(f"Get {buffer_size} data")
            self.batch_processor.process(data_buffer)
            log.debug("Creating new event")
            if self.event.is_set():
                self.event.clear()
            return buffer_size

    def run(self) -> None:
        last_processed_size = 0

        async def trigger(sleep: float):
            try:
                if await asyncio.wait_for(
                        self.event.wait(),
                        timeout=sleep
                ):
                    log.debug("Triggered by size overflow.")
            except asyncio.TimeoutError:
                log.debug("Triggered by time.")

        while not (self.stop and self.queue.qsize() == 0):
            try:
                if last_processed_size != self.batch_processor.get_batch_size():
                    self._loop.run_until_complete(
                        trigger(self.batch_time)
                    )
                    log.debug("Some triggered fired")
                else:
                    log.debug("Busy mode, skip trigger")
                if self.queue.qsize() > 0:
                    log.debug("Start to process data")
                    last_processed_size = self._process()
                else:
                    last_processed_size = 0
                    log.debug("No data to process")
            except Exception as ex:
                log.fatal("Some error happened while doing batch", exc_info=ex)
                time.sleep(1)
