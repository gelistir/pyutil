from unittest import TestCase

import pandas as pd
from nose.tools import raises

from pyutil.message import Mail
from test.config import resource


class TestDecorator(TestCase):
    @raises(AssertionError)
    def test_warning(self):
        m = Mail(mailgunapi="https://api.mailgun.net/v2/maffay.com/messages", mailgunkey="1")
        m.attach_stream("test.csv", pd.Series(index=[1,2], data=[3,4]).to_csv())
        m.inline_stream("test.csv", pd.Series(index=[1,2], data=[3,4]).to_csv())
        m.fromAdr = "Peter Maffay"
        m.toAdr = "David Hasselhoff"
        m.subject = "From Peter with love"
        self.assertEquals(m.fromAdr, "Peter Maffay")
        self.assertEquals(m.toAdr, "David Hasselhoff")
        self.assertEquals(m.subject, "From Peter with love")
        m.send(text="Wurst")

    @raises(AssertionError)
    def test_empty_text(self):
        m = Mail(mailgunapi="https://api.mailgun.net/v2/maffay.com/messages", mailgunkey="1")
        m.fromAdr = "Peter Maffay"
        m.toAdr = "David Hasselhoff"
        m.send()

    @raises(AssertionError)
    def test_html(self):
        m = Mail(mailgunapi="https://api.mailgun.net/v2/maffay.com/messages", mailgunkey="1")
        m.fromAdr = "Peter Maffay"
        m.toAdr = "David Hasselhoff"
        m.subject = "From Peter with love"
        m.send(html="haha")

    def test_attach_stream(self):
        m = Mail(mailgunapi="https://api.mailgun.net/v2/maffay.com/messages", mailgunkey="1")
        m.attach_stream("test.csv", stream=pd.Series(index=[1,2,3], data=[4,5,6]).to_csv())
        x = m.files
        self.assertEquals(len(x), 1)
        self.assertEquals(x[0][0], "attachment")
        self.assertIsNotNone(x[0][1])

    def test_attach_file(self):
        m = Mail(mailgunapi="https://api.mailgun.net/v2/maffay.com/messages", mailgunkey="1")
        file = resource("drawdown.csv")
        m.attach_file("test.csv", localpath=file)
        x = m.files
        self.assertEquals(len(x), 1)
        self.assertEquals(x[0][0], "attachment")
        self.assertIsNotNone(x[0][1])
