import tempfile
from unittest import TestCase

import pandas as pd

from pyutil.message import Mail

import httpretty


class TestDecorator(TestCase):
    @httpretty.activate
    def test_warning(self):
        httpretty.register_uri(
            httpretty.POST,
            "https://api.mailgun.net/v2/maffay.com/messages",
            body='{"origin": "127.0.0.1"}'
        )

        m = Mail(mailgunapi="https://api.mailgun.net/v2/maffay.com/messages", mailgunkey="1")
        m.attach_stream("test.csv", pd.Series(index=[1, 2], data=[3, 4]).to_csv())
        m.inline_stream("test.csv", pd.Series(index=[1, 2], data=[3, 4]).to_csv())
        with tempfile.NamedTemporaryFile(delete=True) as file:
            m.inline_file(name="Peter", localpath=file.name)
            m.attach_file(name="Maffay", localpath=file.name)
            m.text = "Ich wollte nie erwachsen sein..."
            m.fromAdr = "Peter Maffay"
            m.toAdr = "David Hasselhoff"
            m.subject = "From Peter with love"
            self.assertEqual(m.fromAdr, "Peter Maffay")
            self.assertEqual(m.toAdr, "David Hasselhoff")
            self.assertEqual(m.subject, "From Peter with love")

            m.send()

            x = m.files
            self.assertEqual(len(x), 4)

    def test_empty_text(self):
        m = Mail(mailgunapi="https://api.mailgun.net/v2/maffay.com/messages", mailgunkey="1")
        m.fromAdr = "Peter Maffay"
        m.toAdr = "David Hasselhoff"
        with self.assertRaises(AssertionError):
            m.send()
