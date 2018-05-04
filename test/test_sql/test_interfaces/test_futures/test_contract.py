from unittest import TestCase

import pandas as pd

from pyutil.sql.interfaces.futures.category import FuturesCategory
from pyutil.sql.interfaces.futures.contract import Contract
from pyutil.sql.interfaces.futures.exchange import Exchange
from pyutil.sql.interfaces.futures.future import Future

def future():
    # define an exchange
    e = Exchange(name="Chicago Mercantile Exchange", exch_code="CME")
    # define a category
    c = FuturesCategory(name="Equity Index")
    # define the future
    return Future(name="ES1 Index", internal="S&P 500 E-Mini Futures", quandl="CME/ES", exchange=e, category=c)


class TestContract(TestCase):
    def test_contract(self):
        c = Contract(figi="B3BB5", notice=pd.Timestamp("2010-01-01").date(), bloomberg_symbol="AAA", fut_month_yr="MAR 00")
        self.assertEqual(c.notice, pd.Timestamp("2010-01-01").date())
        self.assertEqual(c.bloomberg_symbol, "AAA")
        self.assertEqual(c.fut_month_yr, "MAR 00")
        self.assertTrue(c.alive(today=pd.Timestamp("2009-11-17").date()))
        self.assertFalse(c.alive(today=pd.Timestamp("2010-02-03").date()))
        self.assertEqual(c.discriminator, "Contract")
        self.assertEqual(c.figi, "B3BB5")
        self.assertEqual(c.name, "B3BB5")
        self.assertEqual(c.month_xyz, "MAR")
        self.assertEqual(c.month_x, "H")
        self.assertEqual(c.year, 2000)
        self.assertEqual(str(c), "Contract(B3BB5)")

        c = Contract(figi="B3BB5", notice=pd.Timestamp("1960-01-01").date(), bloomberg_symbol="AAA", fut_month_yr="MAR 60")
        self.assertEqual(c.year, 1960)

    def test_invariance(self):
        c = Contract(figi="B3BB5", notice=pd.Timestamp("2010-01-01").date(), bloomberg_symbol="AAA", fut_month_yr="MAR 00")
        with self.assertRaises(AttributeError):
            c.figi = "NoNoNo"


