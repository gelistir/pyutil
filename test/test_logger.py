from unittest import TestCase
from pyutil.log import MailHandler, StreamHandler, FileHandler, get_logger
from pyutil.message import Mail
from pyutil.config import Configuration

config = Configuration()

class TestMessage(TestCase):
    def test_handler(self):
        logger = get_logger("TEST LOG")

        mail = Mail(toAdr = "test@mailinator.com", fromAdr="test@mailinator.com", subject="test", mailgunapi=config["Mailgun"]["mailgun"], mailgunkey=config["Mailgun"]["mailgunkey"])

        # add a mailhandler
        handler = MailHandler(mail=mail)
        logger.addHandler(handler)

        # add a streamhandler
        handler = StreamHandler(format="%(name)s - %(levelname)s - %(message)s")
        logger.addHandler(handler)

        filehandler = FileHandler(file="/tmp/tmpLog.log", mode="w+")
        logger.addHandler(filehandler)

        logger.error("BIG PROBLEM!")
        self.assertEqual(handler.stream.getvalue(), "TEST LOG - ERROR - BIG PROBLEM!\n")



