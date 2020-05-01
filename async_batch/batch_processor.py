import abc
import logging
import time
from asyncio import Future
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
            max_wait_time: float = 0.5
    ):
        super().__init__()
        self._interval = max_wait_time
        self._batch_processor = batch_processor
        self._stop = False
        self._queue = Queue(maxsize=queue_capacity)
        self._execution_lock = Lock()
        self._count_trigger = Lock()

    def _size_overflow(self):
        return self._queue.qsize() >= self._batch_processor.get_batch_size()

    def stop(self):
        self._stop = True

    def async_submit(self, data) -> Future:
        if not self._stop:
            data_pack = DataPack(data)
            self._queue.put(data_pack)
            locked = self._execution_lock.acquire(blocking=False)
            if locked:
                if self._size_overflow() and self._count_trigger.locked():
                    self._count_trigger.release()
                self._execution_lock.release()
            # else means failed to get lock, batch processor is running
            return data_pack
        else:
            raise Exception("Task queue was stopped to accept task.")

    def _process(self):
        with self._execution_lock:
            self._batch_processor.process([self._queue.get() for _ in range(min(
                self._queue.qsize(),
                self._batch_processor.get_batch_size()
            ))])
            self._count_trigger.acquire(blocking=False)

    def run(self) -> None:
        while not (self._stop and self._queue.qsize() == 0):
            try:
                if self._queue.qsize() < self._batch_processor.get_batch_size():
                    self._count_trigger.acquire(timeout=self._interval)
                if self._queue.qsize() > 0:
                    self._process()
            except Exception as ex:
                log.fatal("Some critical error happened while doing batch", exc_info=ex)
                time.sleep(1)
