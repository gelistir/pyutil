import pandas as pd
import pandas.util.testing as pdt
from pymongo.database import Database
from pyutil.mongo.archive import reader, writer
from test.config import read_frame, test_portfolio, mongoclient
from unittest import TestCase


class TestReader(TestCase):
    @classmethod
    def setUpClass(cls):

        cls.client = mongoclient()
        cls.db = Database(cls.client, "tmp")

        cls.writer = writer("tmp")
        cls.reader = reader("tmp")

        # write assets into test database. Writing is slow!
        assets = read_frame("price.csv", parse_dates=True)

        for asset in assets:
            cls.writer.update_asset(asset, assets[asset])

        frame = read_frame("symbols.csv")
        cls.writer.update_symbols(frame)

        p = test_portfolio()
        cls.writer.update_portfolio("test", p, group="test", comment="test")

    @classmethod
    def tearDownClass(cls):
        cls.client.drop_database(cls.db)

    def test_history(self):
        a = self.reader.history(name="PX_LAST")
        self.assertAlmostEqual(a["B"][pd.Timestamp("2014-07-18").date()], 23454.79, places=5)

    def test_history_series(self):
        a = self.reader.history_series(item="B", name="PX_LAST")
        self.assertAlmostEqual(a[pd.Timestamp("2014-07-18")], 23454.79, places=5)

    def test_unknown_series(self):
        self.assertRaises(AssertionError, self.reader.history_series, item="XYZ", name="PX_LAST")

    def test_close(self):
        x = self.reader.history(items=["A", "B"], name="PX_LAST")
        self.assertAlmostEqual(x["B"][pd.Timestamp("2014-01-14")], 22791.28, places=5)

    def test_symbols(self):
        # enforce the same order with this trick
        a = read_frame("symbols.csv")
        self.assertEqual(len(self.reader.symbols.keys()), len(a.keys()))
        pdt.assert_frame_equal(self.reader.symbols, a[self.reader.symbols.keys()], check_dtype=False)

    def test_strategies(self):
        r = self.reader.portfolios.strategies
        self.assertEqual(r["group"]["test"], "test")

    def test_nav(self):
        r = self.reader.portfolios.nav["test"]
        self.assertAlmostEqual(r[pd.Timestamp("2015-04-22")], 1.0070191775792583, places=5)

    def test_porfolio_none(self):
        p = self.reader.portfolios["abc"]
        assert not p

    def test_portfolio(self):
        d = {x: p for x, p in self.reader.portfolios.items()}
        self.assertListEqual(["test"], list(d.keys()))

    def test_unknown_asset(self):
        self.assertRaises(AssertionError, self.reader.history, name="PX_LAST", items=["XYZ"])

    def test_unknown_series(self):
        self.assertRaises(AssertionError, self.reader.history, name="XYZ", items=["A","B"])
