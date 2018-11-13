import unittest

import pandas as pd
import pandas.util.testing as pdt

from pyutil.sql.interfaces.risk.currency import Currency
from pyutil.sql.interfaces.risk.security import Security
from test.config import test_portfolio

t0 = pd.Timestamp("1978-11-15")
t1 = pd.Timestamp("1978-11-16")


class TestSecurity(unittest.TestCase):
    def test_name(self):
        s = Security(name=100, kiid=5, ticker="AAAAA US Equity")
        c = Currency(name="USD")
        self.assertEqual(s.name, "100")

        self.assertIsNone(s.get_ts("price"))
        self.assertEqual(s.discriminator, "Security")
        self.assertEqual(s.kiid, 5)

        self.assertEqual(s.bloomberg_ticker, "AAAAA US Equity")
        self.assertEqual(str(s), "Security(100: None)")
        self.assertEqual(s.bloomberg_scaling, 1.0)

    def test_reference_frame(self):
        s1 = Security(name=100, kiid=4)
        s2 = Security(name=110, kiid=3)
        s3 = Security(name=120, kiid=5)

        x = pd.DataFrame(index=["100", "110", "120"], columns=["KIID"], data=[[4],[3],[5]])
        x.index.name = "Product"
        pdt.assert_frame_equal(x, Security.reference_frame(products=sorted([s1, s2, s3]), name="Product"))

    def test_ts(self):
        security = Security(name="A")

        # update with a series containing a NaN
        self.assertIsNone(security.last(field="price"))

        # upsert series
        security.ts["price"] = test_portfolio().prices["A"]

        # extract the series again
        pdt.assert_series_equal(security.ts["price"], test_portfolio().prices["A"].dropna(), check_names=False)

        # extract the last stamp
        self.assertEqual(security.last(field="price"), pd.Timestamp("2015-04-22"))

        # test json
        a = security.to_json()
        assert isinstance(a, dict)
        self.assertEqual(a["name"], "A")
        pdt.assert_series_equal(a["Price"], security.ts["price"])

