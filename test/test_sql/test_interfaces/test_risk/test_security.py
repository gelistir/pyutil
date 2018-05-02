import unittest
import pandas as pd


import pandas.util.testing as pdt

from pyutil.sql.interfaces.risk.currency import Currency
from pyutil.sql.interfaces.risk.security import Security
from pyutil.sql.interfaces.risk.security import FIELDS as FIELDSSECURITY

t1 = pd.Timestamp("1978-11-16")
t2 = pd.Timestamp("1978-11-18")

KIID = FIELDSSECURITY["Lobnek KIID"]
TICKER = FIELDSSECURITY["Lobnek Ticker Symbol Bloomberg"]


class TestSecurity(unittest.TestCase):
    def test_security(self):
        s1 = Security(entity_id=123)

        self.assertEqual(s1.entity_id, 123)
        self.assertEqual(str(s1), "Security(123)")

        s1.price_upsert(ts={t1: 11.1, t2: 12.1})
        pdt.assert_series_equal(s1.price, pd.Series({t1: 11.1, t2: 12.1}))

        c = Currency(name="USD")
        s1.volatility_upsert(ts={t1: 11.1, t2: 12.1}, currency=c)
        pdt.assert_frame_equal(s1.volatility, pd.DataFrame(index=[t1, t2], columns=[c], data=[[11.1],[12.1]]))

        s1.reference[KIID] = "5"
        self.assertEqual(s1.reference[KIID], 5)
        self.assertEqual(s1.kiid, 5)

        # test the ticker!
        self.assertIsNone(s1.bloomberg_ticker)
        s1.reference[TICKER] = "HAHA US Equity"
        self.assertEqual(s1.bloomberg_ticker, "HAHA US Equity")
