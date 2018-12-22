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
        s = Security(name=100)
        self.assertEqual(s.name, "100")

        self.assertIsNone(s._price)
        self.assertEqual(s.discriminator, "Security")
        self.assertEqual(str(s), "Security(100: None)")

    def test_ts_new(self):
        security = Security(name="A")
        self.assertIsNone(security._price)

        # upsert series
        security._price = test_portfolio().prices["A"]

        # extract the series again
        pdt.assert_series_equal(security._price, test_portfolio().prices["A"].dropna(), check_names=False)

        # test json
        a = security.to_json()
        assert isinstance(a, dict)
        self.assertEqual(a["name"], "A")

        pdt.assert_series_equal(a["Price"], security._price)

        # extract the last stamp
        self.assertEqual(security._price.last_valid_index(), pd.Timestamp("2015-04-22"))

        x = security.upsert_price(ts = 2*test_portfolio().prices["A"])
        pdt.assert_series_equal(x, 2*test_portfolio().prices["A"].dropna(), check_names=False)

    def test_volatility(self):
        security = Security(name="A")
        # short cut using protected variables, don't do it...
        security._vola[Currency(name="USD")] = pd.Series([30, 40, 50])
        security._vola[Currency(name="CHF")] = pd.Series([10, 11, 12])

        pdt.assert_frame_equal(security.vola_frame, pd.DataFrame({key: item for (key, item) in security._vola.items()}))

    def test_volatility(self):
        security = Security(name="B")
        pdt.assert_series_equal(security._vola.get(Currency(name="USD"), default=pd.Series({})), pd.Series({}))

        x = security.upsert_volatility(currency=Currency(name="CHF"), ts=pd.Series([10, 20, 30]))
        pdt.assert_frame_equal(security.vola_frame, pd.DataFrame({Currency(name="CHF"): pd.Series([10,20,30])}))
        pdt.assert_series_equal(x, pd.Series([10, 20, 30]))

