import logging
import sys
import time
from contextlib import ExitStack


def _logger(name="LWM", level=None, format=None):
    __format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    __level = logging.DEBUG

    logger = logging.getLogger(name)
    logger.setLevel(level=level or __level)

    formatter = logging.Formatter(fmt=format or __format)

    # add handler for console output
    handler = logging.StreamHandler(stream=sys.stdout)
    handler.setFormatter(formatter)
    handler.setLevel(logging.DEBUG)

    logger.addHandler(handler)
    return logger



class Production(ExitStack):
    def __init__(self):
        super().__init__()
        self.__logger = _logger()
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

