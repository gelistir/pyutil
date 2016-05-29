from pyutil.log import get_logger, StreamHandler
from unittest import TestCase


class TestLog(TestCase):
    def test_log(self):
        logger = get_logger("Peter")
        logger.addHandler(StreamHandler())
        logger.warning("In da house")