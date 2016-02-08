import os

from unittest import TestCase
from pyutil.message import Mail
from pyutil.config import Configuration

config = Configuration()


class TestMessage(TestCase):
    def test_send(self):
        m = Mail(fromAdr="a", toAdr="b", subject="test", mailgunapi=config["Mailgun"]["mailgun"], mailgunkey=config["Mailgun"]["mailgunkey"])
        m.send("test")

    def test_attach(self):
        m = Mail(fromAdr="a", toAdr="b", subject="test", mailgunapi=config["Mailgun"]["mailgun"], mailgunkey=config["Mailgun"]["mailgunkey"])
        basedir = os.path.dirname(__file__)
        m.attach("test.csv", localpath=os.path.join(basedir, "resources", "client.csv"))
        m.send("test")

    def test_inline(self):
        m = Mail(fromAdr="a", toAdr="b", subject="test", mailgunapi=config["Mailgun"]["mailgun"], mailgunkey=config["Mailgun"]["mailgunkey"])
        basedir = os.path.dirname(__file__)
        m.inline("test.csv", localpath=os.path.join(basedir, "resources", "client.csv"))
        m.send("test")
