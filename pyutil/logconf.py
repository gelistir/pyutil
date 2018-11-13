# We create a standard logger with two handlers
# One of them is using io.StringIO for streaming
# Subsequently we add the value() method to the logger, e.g. it will be possible to call
# logger.value() and get the entire history of all messages sent to the logger.
#
# I have never managed to get my head around logging in Python. Any comments are very appreciated.

import logging
import types

from io import StringIO

__format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
__stream = StringIO()
__formatter = logging.Formatter(fmt=__format)

sysout_handler = logging.StreamHandler()
sysout_handler.setLevel(logging.DEBUG)
sysout_handler.setFormatter(__formatter)

StringIO_handler = logging.StreamHandler(stream=__stream)
StringIO_handler.setLevel(logging.DEBUG)
StringIO_handler.setFormatter(__formatter)

logger = logging.getLogger("")
logger.setLevel(level=logging.DEBUG)

logger.addHandler(sysout_handler)
logger.addHandler(StringIO_handler)


# https://stackoverflow.com/questions/972/adding-a-method-to-an-existing-object-instance
def value(self):
    __stream.flush()
    return __stream.getvalue()


logger.value = types.MethodType(value, logger)


# from ... import logger
#
# if __name__ == '__main__':
#     logger.info("Hello World")
#     logger.info("Hello World again!")
#
#     print(logger.value())
#
