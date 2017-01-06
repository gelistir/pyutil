import pandas as pd
from pyutil.mongo.mongoArchive import MongoArchive

from test.config import read_frame, test_portfolio
from unittest import TestCase
import pandas.util.testing as pdt
from nose.tools import raises

class TestAssets(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.archive = MongoArchive()
        frame = read_frame("price.csv", parse_dates=True)
        cls.archive.assets.update_all(frame=frame)
        cls.assets = cls.archive.assets
        cls.assets.update("A", ts=frame["A"].tail(10))

    def test_Keys(self):
        self.assertListEqual(self.assets.keys(), ['A', 'B', 'C', 'D', 'E', 'F', 'G'])

    def test_history(self):
        a = self.archive.history(name="PX_LAST", assets=["A", "B"])
        self.assertAlmostEqual(a["B"]["2014-07-18"], 23454.79, places=5)

        a = self.archive.history(name="PX_LAST")
        self.assertAlmostEqual(a["B"]["2014-07-18"], 23454.79, places=5)

    def test_assets_item(self):
        a = self.assets["B"]["PX_LAST"]
        self.assertAlmostEqual(a["2014-07-18"], 23454.79, places=5)

    def test_unknown_series(self):
        with self.assertRaises(AssertionError):
            self.archive.history(assets=["XYZ"], name="PX_LAST")

    def test_update(self):
        self.assets.update(asset="B", ts=pd.Series(index=["2016-07-18"], data=[1.0]))
        self.assertAlmostEqual(self.assets["B"]["PX_LAST"]["2016-07-18"], 1.0, places=10)

    def test_unknown_series_warning(self):
        with self.assertWarns(Warning):
            self.archive.history(assets=["A", "B"], name="XYZ")

    def test_set(self):
        self.archive.assets["TEST"] = self.archive.assets["B"]
        #print(self.archive.assets.keys())
        self.assertAlmostEqual(self.assets["TEST"]["PX_LAST"]["2014-07-18"], 23454.79, places=5)

        del self.archive.assets["TEST"]
        print(self.archive.assets.keys())



class TestFrames(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.archive = MongoArchive()
        cls.archive.frames["Peter Maffay"] = pd.DataFrame(columns=["A", "B"], data=[[1.2, 2.5]])
        cls.frames = cls.archive.frames

    def test_frame(self):
        x = self.archive.frames["Peter Maffay"]
        pdt.assert_frame_equal(x, pd.DataFrame(columns=["A", "B"], data=[[1.2, 2.5]]))

    def test_multiindex_1(self):
        tuples = [("Maffay", "X"), ("Maffay", "Y"), ("Peter", "A"), ("Peter", "B")]
        index = pd.MultiIndex.from_tuples(tuples=tuples, names=["number", "color"])
        x = pd.DataFrame(columns=["C1"], index=index, data=[[2], [3], [0], [1]])
        self.archive.frames["MyFrame"] = x
        pdt.assert_frame_equal(self.archive.frames["MyFrame"], x)
        del self.archive.frames["MyFrame"]

    def test_multiindex_2(self):
        x = pd.DataFrame(columns=["C1"], index=["A","B"], data=[[2], [3]])
        self.archive.frames["MyFrame"] = x
        pdt.assert_frame_equal(self.archive.frames["MyFrame"], x)
        del self.archive.frames["MyFrame"]

    @raises(AssertionError)
    def test_multiindex_3(self):
        tuples = [("Maffay", "X"), ("Maffay", "Y"), ("Peter", "A"), ("Peter", "B")]
        index = pd.MultiIndex.from_tuples(tuples=tuples)
        x = pd.DataFrame(columns=["C1"], index=index, data=[[2], [3], [0], [1]])
        self.archive.frames["MyFrame"] = x

    def test_del_frame(self):
        self.frames["Peter"] = pd.DataFrame()
        self.assertTrue("Peter" in self.frames.keys())
        del self.frames["Peter"]
        self.assertTrue("Peter" not in self.frames.keys())

class TestSymbols(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.archive = MongoArchive()
        cls.archive.symbols.update_all(frame=read_frame("symbols.csv"))

    def test_frame(self):
        s = self.archive.symbols.frame
        self.assertEqual(s["group"]["A"], "Alternatives")

    def test_item(self):
        s = self.archive.symbols["A"]
        self.assertEqual(s["group"], "Alternatives")

    def test_keys(self):
        self.assertListEqual(self.archive.symbols.keys(), ['A', 'B', 'C', 'D', 'E', 'F', 'G'])

    def test_set(self):
        self.archive.symbols["T"] = {"prop1": "2.0", "prop2": "Peter Maffay"}
        g = self.archive.symbols["T"]
        self.assertEqual(g["prop2"], "Peter Maffay")

        self.archive.symbols["T"] = {"prop3": "2.0", "prop2": "Peter Maffay"}
        self.assertTrue("prop1" not in self.archive.symbols["T"].index)
        self.assertTrue("prop2" in self.archive.symbols["T"].index)

        del self.archive.symbols["T"]


class TestPortfolio(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.archive = MongoArchive()
        # need this for sector-weights
        cls.archive.symbols.update_all(frame=read_frame("symbols.csv"))
        cls.archive.portfolios.update("test", test_portfolio(), group="test", comment="test")

    def test_symbols(self):
        r = self.archive.portfolios.strategies
        self.assertEqual(r["group"]["test"], "test")

    def test_nav(self):
        r = self.archive.portfolios.nav["test"]
        # test the nav
        self.assertAlmostEqual(r["2015-04-22"], 1.0070191775792583, places=5)

    def test_porfolio_none(self):
        p = self.archive.portfolios["abc"]
        assert not p

    def test_sector_weights(self):
        symbolmap = self.archive.symbols.frame["group"]
        sector_w = self.archive.portfolios.sector_weights("test", symbolmap)
        self.assertAlmostEqual(sector_w["Equity"]["2013-01-04"], 0.24351702703439526, places=5)

    def test_update(self):
        portfolio = test_portfolio()
        self.archive.portfolios.update(key="test", portfolio=portfolio.tail(10), group="test", comment="test")

        g = self.archive.portfolios["test"]
        pdt.assert_frame_equal(portfolio.prices, g.prices)
        pdt.assert_frame_equal(portfolio.weights, g.weights)