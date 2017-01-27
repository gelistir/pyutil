import pandas as pd
from pyutil.mongo.mongoArchive import MongoArchive

from test.config import read_frame, test_portfolio
from unittest import TestCase
import pandas.util.testing as pdt


prices = read_frame("price.csv", parse_dates=True)
symbols = read_frame("symbols.csv")

class TestMongoArchive(TestCase):
    @classmethod
    def setUp(self):
        self.archive = MongoArchive()
        self.archive.drop()
        self.archive.symbols.update_all(frame=symbols)
        self.archive.assets.update_all(frame=prices)

    def test_history(self):
        pdt.assert_frame_equal(self.archive.history(), prices)
        pdt.assert_frame_equal(self.archive.history(name="PX_LAST"), prices)
        pdt.assert_frame_equal(self.archive.history(assets=["A", "B"]), prices[["A","B"]])

    def test_symbols(self):
        pdt.assert_frame_equal(self.archive.reference.sort_index(axis=1), symbols.sort_index(axis=1), check_dtype=False)

    def test_unknown_series(self):
        with self.assertRaises(AssertionError):
            self.archive.history(assets=["XYZ"], name="PX_LAST")

    def test_unknown_series_warning(self):
        with self.assertWarns(Warning):
            self.archive.history(assets=["A", "B"], name="XYZ")

    def test_asset(self):
        a = self.archive.asset("A")
        self.assertEquals(a.reference["internal"], "Gold")


class TestAssets(TestCase):
    @classmethod
    def setUp(self):
        self.archive = MongoArchive()
        self.archive.drop()
        self.archive.symbols.update_all(frame=symbols)
        self.archive.assets.update_all(frame=prices)

    def test_Keys(self):
        self.assertListEqual(list(self.archive.assets.keys()), ['A', 'B', 'C', 'D', 'E', 'F', 'G'])

    def test_assets_item(self):
        pdt.assert_series_equal(self.archive.assets["B"]["PX_LAST"], prices["B"].dropna(), check_names=False)

    def test_update(self):
        # update database, make sure you delete entry again
        self.archive.assets.update(asset="B", ts=pd.Series(index=[pd.Timestamp("2016-07-07")], data=1.0))
        self.assertAlmostEqual(self.archive.assets["B"]["PX_LAST"][pd.Timestamp("2016-07-07")], 1.0, places=10)

    def test_set(self):
        with self.assertRaises(NotImplementedError):
            self.archive.assets["A"] = 0

class TestSymbols(TestCase):
    @classmethod
    def setUp(self):
        self.archive = MongoArchive()
        self.archive.drop()
        self.archive.symbols.update_all(frame=symbols)

    def test_frame(self):
        s = self.archive.symbols.frame
        self.assertEqual(s["group"]["A"], "Alternatives")

    def test_item(self):
        s = self.archive.symbols["A"]
        self.assertEqual(s["group"], "Alternatives")

    def test_keys(self):
        self.assertListEqual(list(self.archive.symbols.keys()), ['A', 'B', 'C', 'D', 'E', 'F', 'G'])

    def test_set(self):
        self.archive.symbols["T"] = {"prop1": "2.0", "prop2": "Peter Maffay"}
        g = self.archive.symbols["T"]
        self.assertEqual(g["prop2"], "Peter Maffay")

        self.archive.symbols["T"] = {"prop3": "2.0", "prop2": "Peter Maffay"}
        self.assertTrue("prop1" not in self.archive.symbols["T"].index)
        self.assertTrue("prop2" in self.archive.symbols["T"].index)

        #del self.archive.symbols["T"]

    def test_drop(self):
        self.archive.symbols.drop()
        self.assertTrue(self.archive.symbols.empty, msg="There are no symbols left in the database")
        self.archive.symbols.update_all(frame=read_frame("symbols.csv"))

    def test_del_unknown(self):
        result = self.archive.symbols.remove("Peter Maffay is in da house")
        self.assertEqual(result["n"], 0, "No element has been deleted")


class TestPortfolio(TestCase):
    def setUp(self):
        self.archive = MongoArchive()
        self.archive.drop()

        # need this for sector-weights
        self.archive.symbols.update_all(frame=symbols)
        p = test_portfolio(group="test", comment="test", time=pd.Timestamp("1980-01-01"))
        self.archive.portfolios.update("test", p)

    def test_get(self):
        p = self.archive.portfolios["test"]
        self.assertDictEqual(p.meta, {'comment': 'test', 'time': pd.Timestamp("01-01-1980"), 'group': 'test'})

    def test_porfolio_none(self):
        p = self.archive.portfolios["abc"]
        assert not p

    def test_update(self):
        portfolio = test_portfolio()
        self.archive.portfolios.update(key="test", portfolio=portfolio.tail(10))

        g = self.archive.portfolios["test"]
        pdt.assert_frame_equal(portfolio.prices, g.prices)
        pdt.assert_frame_equal(portfolio.weights, g.weights)