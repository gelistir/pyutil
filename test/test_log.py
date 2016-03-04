import os
from pyutil.config import mail
from pyutil.log import get_logger, FileHandler, StreamHandler, MailHandler
from unittest import TestCase
from tempfile import NamedTemporaryFile


class TestLog(TestCase):
    def test_log(self):
        logger = get_logger("Peter")
        f = NamedTemporaryFile()
        logger.addHandler(FileHandler(file=f.name))
        logger.addHandler(StreamHandler())

        m = mail()
        m.toAdr = "Peter.Maffay@mailinator.com"
        m.fromAdr = "Peter.Maffay@mailinator.com"
        logger.addHandler(MailHandler(m))

        logger.warning("In da house")

        os.path.exists(f.name)


