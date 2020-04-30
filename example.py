import logging
import time
from typing import Iterable

import uvicorn
from fastapi import FastAPI
from starlette.responses import RedirectResponse

from async_batch.batch_processor import BatchProcessor, TaskQueue

log = logging.getLogger(__file__)
logging.basicConfig(level=logging.INFO)

app = FastAPI(
    title="Async Batcher Example Project",
    version="0.1",
    description="Async Batch Project"
)


class ExampleBatchProcessor(BatchProcessor):
    def _process(self, batch_data: Iterable[int]) -> Iterable[int]:
        return [x ** 2 for x in batch_data]

    def __init__(self, batch_size: int):
        self.batch_size = batch_size

    def get_batch_size(self) -> int:
        return self.batch_size


tq = TaskQueue(
    batch_processor=ExampleBatchProcessor(2),
    batch_time=3
)


@app.get("/")
async def read_root():
    """
    Got to document
    """
    return RedirectResponse("docs")


@app.get("/test")
async def test_api(number: int):
    start_time = time.time()
    data = await tq.async_submit(number)
    spent = time.time() - start_time
    return {
        "status": "success",
        "result": data,
        "used_time": spent
    }


if __name__ == '__main__':
    # logging.basicConfig(format=FORMAT,level=logging.DEBUG)
    LOGGING_CONFIG = {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {
                "()": "uvicorn.logging.DefaultFormatter",
                "fmt": "%(asctime)-15s  %(levelprefix)s %(message)s",
                "use_colors": None,
            },
            "access": {
                "()": "uvicorn.logging.AccessFormatter",
                "fmt": '%(asctime)-15s  %(levelprefix)s %(client_addr)s - "%(request_line)s" %(status_code)s',
            },
        },
        "handlers": {
            "default": {
                "formatter": "default",
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stderr",
            },
            "access": {
                "formatter": "access",
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stdout",
            },
        },
        "loggers": {
            "": {"handlers": ["default"], "level": "DEBUG"},
            "uvicorn.error": {"level": "DEBUG"},
            "uvicorn.access": {"handlers": ["access"], "level": "DEBUG", "propagate": False},
        },
    }
    tq.start()
    uvicorn.run(app, log_config=LOGGING_CONFIG)
