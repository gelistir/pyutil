from unittest import TestCase

import pandas as pd

from pyutil.message import Mail


class TestDecorator(TestCase):
    def test(self):
        m = Mail(mailgunapi="1", mailgunkey="1")
        m.attach_stream("test.csv", pd.Series(index=[1,2], data=[3,4]).to_csv())
        m.fromAdr = "Peter Maffay"
        m.toAdr = "David Hasselhoff"
        m.subject = "From Peter with love"
        self.assertEquals(m.fromAdr, "Peter Maffay")
        self.assertEquals(m.toAdr, "David Hasselhoff")
        self.assertEquals(m.subject, "From Peter with love")