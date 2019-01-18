import tempfile

import httpretty
import pandas as pd
import pytest

from pyutil.message import Mail


class TestMessage(object):
    @httpretty.activate
    def test_warning(self):
        httpretty.register_uri(
            httpretty.POST,
            "https://api.mailgun.net/v2/maffay.com/messages",
            body='{"origin": "127.0.0.1"}'
        )

        m = Mail(mailgunapi="https://api.mailgun.net/v2/maffay.com/messages")
        m.attach_stream("test.csv", pd.Series(index=[1, 2], data=[3, 4]).to_csv())
        m.inline_stream("test.csv", pd.Series(index=[1, 2], data=[3, 4]).to_csv())
        with tempfile.NamedTemporaryFile(delete=True) as file:
            m.inline_file(name="Peter", localpath=file.name)
            m.attach_file(name="Maffay", localpath=file.name)
            m.text = "Ich wollte nie erwachsen sein..."
            m.fromAdr = "Peter Maffay"
            m.toAdr = "David Hasselhoff"
            m.subject = "From Peter with love"
            assert m.fromAdr == "Peter Maffay"
            assert m.toAdr == "David Hasselhoff"
            assert m.subject == "From Peter with love"

            m.send(html=True)

            x = m.files
            assert len(x) == 4

    def test_empty_text(self):
        m = Mail()
        m.fromAdr = "Peter Maffay"
        m.toAdr = "David Hasselhoff"
        # you can't send anything with fake mailgunapi and mailgunkey
        with pytest.raises(AssertionError):
            m.send()
