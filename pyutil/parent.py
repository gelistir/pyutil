import logging
import time

#import pandas as pd
# @contextlib.contextmanager
# def parent(log=None):
#     log = log or logging.getLogger(__name__)
#     try:
#         yield log
#     except Exception as e:
#         log.exception(e)
#         raise
#     finally:
#         log.debug("Bye")
import pandas as pd


class Parent(object):
    # see http://arnavk.com/posts/python-context-managers/
    def __init__(self, log=None, width=None):
        self.__logger = log or logging.getLogger(__name__)
        self.__time = time.time()
        pd.options.display.width = (width or 250)

    @property
    def logger(self):
        return self.__logger

    @property
    def elapsed(self):
        return time.time() - self.__time

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type is not None:
            self.logger.exception(exc_val)
            return False

        self.logger.info("Time elapsed: {0}".format(self.elapsed))
        return True
