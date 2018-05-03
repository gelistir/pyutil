from unittest import TestCase

import pandas as pd

from pyutil.sql.interfaces.futures.category import FuturesCategory
from pyutil.sql.interfaces.futures.contract import Contract
from pyutil.sql.interfaces.futures.exchange import Exchange
from pyutil.sql.interfaces.futures.future import Future, Futures
from pyutil.sql.model.ref import Field, DataType, FieldType


def future():
    # define an exchange
    e = Exchange(name="Chicago Mercantile Exchange", exch_code="CME")
    # define a category
    c = FuturesCategory(name="Equity Index")
    # define the future
    return Future(name="ES1 Index", internal="S&P 500 E-Mini Futures", quandl="CME/ES", exchange=e, category=c)


class TestFuture(TestCase):
    def test_exchange(self):
        e = Exchange(name="Chicago Mercantile Exchange", exch_code="CME")
        self.assertEqual(e.name, "Chicago Mercantile Exchange")
        self.assertEqual(e.exch_code, "CME")

    def test_category(self):
        c = FuturesCategory(name="Oil")
        self.assertEqual(c.name, "Oil")

    def test_future(self):
        f = future()
        self.assertEqual(f.name, "ES1 Index")
        #self.assertEqual(f.category.name, "Equity Index")
        #self.assertEqual(f.exchange.name, "Chicago Mercantile Exchange")
        self.assertEqual(f.internal, "S&P 500 E-Mini Futures")
        self.assertEqual(f.quandl, "CME/ES")
        self.assertEqual(str(f), "Future(ES1 Index)")

    def test_future_ts(self):
        f = future()
        # test with time series data
        f.upsert_ts(name="price", data={pd.Timestamp("2018-03-14").date(): 20.0})
        self.assertEqual(f.timeseries["price"][pd.Timestamp("2018-03-14").date()], 20.0)

    def test_future_reference(self):
        f = future()
        # test with reference data
        field = Field(name="trades", result=DataType.integer, type=FieldType.dynamic)
        f.reference[field] = "100"
        self.assertEqual(f.reference[field], 100)

    def test_contract(self):
        f = future()
        c = Contract(future=f, notice=pd.Timestamp("2000-03-16"), figi="B1")
        c.bloomberg_symbol = "ESH00 Index"
        c.fut_month_yr = "MAR 00"
        self.assertListEqual(f.contracts, [c])

        self.assertEqual(c.future, f)
        self.assertEqual(c.bloomberg_symbol, "ESH00 Index")
        self.assertFalse(c.alive(today=pd.Timestamp("2001-02-15")))
        self.assertTrue(c.alive(today=pd.Timestamp("2000-01-15")))

        self.assertEqual(c.figi, "B1")
        self.assertEqual(c.fut_month_yr, "MAR 00")
        self.assertEqual(c.month_x, "H")
        self.assertEqual(c.month_xyz, "MAR")
        self.assertEqual(c.quandl, "CME/ESH2000")
        self.assertEqual(c.year, 2000)

        self.assertEqual(f.max_notice, pd.Timestamp("2000-03-16"))
        self.assertListEqual(f.figis, ["B1"])

    def test_future_no_contracts(self):
        # define an exchange
        e = Exchange(name="Chicago Mercantile Exchange", exch_code="CME")
        c = FuturesCategory(name="Agricultural Products")
        f = Future(name="C 1 Comdty", exchange=e, category=c)

        self.assertIsNone(f.max_notice)
        self.assertListEqual(f.contracts, [])


class TestFutures(TestCase):
    def test_futures(self):
        # create a future
        f = future()
        # make a "dictionary" of futures
        x = Futures([f])
        self.assertEqual(x["ES1 Index"], f)

    def test_futures_wrong_argument(self):
        f = 2.0
        with self.assertRaises(AssertionError):
            Futures([f])
