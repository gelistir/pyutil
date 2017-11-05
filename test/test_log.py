import logging
from tempfile import NamedTemporaryFile
from unittest import TestCase

from pyutil.log import get_logger, StreamHandler, FileHandler, SentryHandler


class TestLog(TestCase):
    def test_log(self):
        logger = get_logger("Peter")
        logger.addHandler(StreamHandler())
        logger.warning("In da house")


    def test_file_handler(self):
        with NamedTemporaryFile(delete=True) as file:
            h = FileHandler(file=file.name)
            self.assertEqual(h.level, logging.INFO)

    def test_sentry_handler(self):
        h = SentryHandler(dns="https://bbbbb:aaaaa@sentry.io/11111")
        self.assertEqual(h.level, logging.WARNING)


