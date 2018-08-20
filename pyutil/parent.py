import logging
import sys
import time
from contextlib import ExitStack

import io


def run_jobs(jobs, logger=None):
    logger = logger or logging.getLogger(__name__)
    for job in jobs:
        logger.info("Job {j}".format(j=job))
        job.start()

    for job in jobs:
        logger.info("Wait for job {j}".format(j=job))
        job.join()


def _get_stream_handler(level=None, format=None, stream=sys.stdout):
    __format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    __level = logging.DEBUG

    formatter = logging.Formatter(fmt=format or __format)
    level = level or __level

    handler = logging.StreamHandler(stream=stream)
    handler.setFormatter(formatter)
    handler.setLevel(level)

    return handler


def _get_logger(name="LWM", level=None):
    __level = logging.DEBUG

    logger = logging.getLogger(name)
    logger.setLevel(level=level or __level)

    return logger


class Production(ExitStack):
    def __init__(self, name="LWM", level=None, format=None):
        super().__init__()
        self.__stream = io.StringIO()
        self.__logger = _get_logger(name=name, level=level)
        self.__logger.addHandler(_get_stream_handler(level=level, format=format))
        self.__logger.addHandler(_get_stream_handler(level=level, stream=self.__stream, format=format))
        self.__time = time.time()

    @property
    def logger(self):
        return self.__logger

    @property
    def elapsed(self):
        return time.time() - self.__time

    def __exit__(self, exc_type, exc_val, exc_tb):
        super().__exit__(exc_type, exc_val, exc_tb)
        if exc_type is not None:
            self.logger.exception(exc_val)
            return False

        self.logger.info("Time elapsed: {0}".format(self.elapsed))
        return True

    @property
    def log_stream(self):
        self.__stream.flush()
        return self.__stream.getvalue()
