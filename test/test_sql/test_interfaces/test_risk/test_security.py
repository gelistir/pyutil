import unittest

import pandas as pd
import pandas.util.testing as pdt

from pyutil.sql.interfaces.risk.custodian import Currency
from pyutil.sql.interfaces.risk.security import Security
from test.config import test_portfolio

t0 = pd.Timestamp("1978-11-15")
t1 = pd.Timestamp("1978-11-16")


class TestSecurity(unittest.TestCase):
    def test_name(self):
        s = Security(name=100, kiid=5, ticker="AAAAA US Equity")
        c = Currency(name="USD")
        self.assertEqual(s.name, "100")

        self.assertIsNone(s.price)
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


    def test_ts_new(self):
        security = Security(name="A")

        self.assertIsNone(security.price)

        # upsert series
        security.price = test_portfolio().prices["A"]

        # extract the series again
        pdt.assert_series_equal(security.price, test_portfolio().prices["A"].dropna(), check_names=False)

        # test json
        a = security.to_json()
        assert isinstance(a, dict)
        self.assertEqual(a["name"], "A")

        pdt.assert_series_equal(a["Price"], security.price)

        # extract the last stamp
        self.assertEqual(security.price.last_valid_index(), pd.Timestamp("2015-04-22"))


    def test_volatility(self):
        security = Security(name="A")
        security.vola[Currency(name="USD")] = pd.Series([30,40,50])
        security.vola[Currency(name="CHF")] = pd.Series([10,11,12])

        pdt.assert_frame_equal(security.vola_frame, pd.DataFrame({key: item for (key, item) in security.vola.items()}))


    def test_volatility_2(self):
        security = Security(name="B")
        pdt.assert_series_equal(security.vola.get(Currency(name="USD"), default=pd.Series({})), pd.Series({}))

        security.upsert_volatility(currency=Currency(name="CHF"), ts=pd.Series([10,20,30]))
        pdt.assert_series_equal(security.vola[Currency(name="CHF")], pd.Series([10,20,30]))
