from builtins import Warning, AssertionError

import pandas as pd
from pyutil.mongo.archive import reader
from test.config import read_frame, test_portfolio
from unittest import TestCase
import pandas.util.testing as pdt
from nose.tools import raises

class TestAssets(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.archive = reader("mongo", host="mongo")
        cls.archive.assets.update_all(frame=read_frame("price.csv", parse_dates=True))
        cls.assets = cls.archive.assets

    def test_Keys(self):
        self.assertListEqual(self.assets.keys(), ['A', 'B', 'C', 'D', 'E', 'F', 'G'])

    def test_history(self):
        a = self.archive.history(name="PX_LAST", items=["A","B"])
        self.assertAlmostEqual(a["B"][pd.Timestamp("2014-07-18").date()], 23454.79, places=5)

        a = self.archive.history(name="PX_LAST")
        self.assertAlmostEqual(a["B"][pd.Timestamp("2014-07-18").date()], 23454.79, places=5)

    def test_history_series(self):
        a = self.archive.history_series(item="B", name="PX_LAST")
        self.assertAlmostEqual(a[pd.Timestamp("2014-07-18")], 23454.79, places=5)

    def test_assets_item(self):
        a = self.archive.assets["B"]["PX_LAST"]
        self.assertAlmostEqual(a[pd.Timestamp("2014-07-18")], 23454.79, places=5)

    def test_unknown_series(self):
        with self.assertRaises(AssertionError):
            self.archive.history_series(item="XYZ", name="PX_LAST")

    def test_update(self):
        self.assets.update(asset="B", ts=pd.Series(index=[pd.Timestamp("2016-07-18")], data=[1.0]))
        self.assertAlmostEqual(self.assets["B"]["PX_LAST"][pd.Timestamp("2016-07-18")], 1.0, places=10)

    def test_unknown_series_warning(self):
        with self.assertWarns(Warning):
            self.archive.history(items=["A","B"], name="XYZ")

class TestFrames(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.archive = reader("mongo", host="mongo")
        cls.archive.frames["Peter Maffay"] = pd.DataFrame(columns=["A", "B"], data=[[1.2, 2.5]])
        cls.frames = cls.archive.frames

    def test_frame(self):
        x = self.archive.frames["Peter Maffay"]
        pdt.assert_frame_equal(x, pd.DataFrame(columns=["A", "B"], data=[[1.2, 2.5]]))

        assert "Peter Maffay" in self.archive.frames.keys()
        assert len(self.archive.frames.keys())==1

        pair = self.archive.frames.items()[0]
        self.assertEqual(pair[0], "Peter Maffay")

    def test_multiindex_1(self):
        tuples = [("Maffay", "X"), ("Maffay", "Y"), ("Peter", "A"), ("Peter", "B")]
        index = pd.MultiIndex.from_tuples(tuples=tuples, names=["number", "color"])
        x = pd.DataFrame(columns=["C1"], index=index, data=[[2], [3], [0], [1]])
        self.archive.frames["MyFrame"] = x
        pdt.assert_frame_equal(self.archive.frames["MyFrame"], x)

    def test_multiindex_2(self):
        x = pd.DataFrame(columns=["C1"], index=["A","B"], data=[[2], [3]])
        self.archive.frames["MyFrame"] = x
        pdt.assert_frame_equal(self.archive.frames["MyFrame"], x)

    @raises(AssertionError)
    def test_multiindex_3(self):
        tuples = [("Maffay", "X"), ("Maffay", "Y"), ("Peter", "A"), ("Peter", "B")]
        index = pd.MultiIndex.from_tuples(tuples=tuples)
        x = pd.DataFrame(columns=["C1"], index=index, data=[[2], [3], [0], [1]])
        self.archive.frames["MyFrame"] = x


class TestSymbols(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.archive = reader("mongo", host="mongo")
        cls.archive.symbols.update_all(frame=read_frame("symbols.csv"))

    def test_frame(self):
        s = self.archive.symbols.frame
        self.assertEqual(s["group"]["A"], "Alternatives")

    def test_item(self):
        s = self.archive.symbols["A"]
        self.assertEqual(s["group"], "Alternatives")

    def test_keys(self):
        self.assertListEqual(self.archive.symbols.keys(), ['A', 'B', 'C', 'D', 'E', 'F', 'G'])



class TestPortfolio(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.archive = reader("mongo", host="mongo")
        # need this for sector-weights
        cls.archive.symbols.update_all(frame=read_frame("symbols.csv"))
        cls.archive.portfolios.update("test", test_portfolio(), group="test", comment="test")


    def test_symbols(self):
        r = self.archive.portfolios.strategies
        self.assertEqual(r["group"]["test"], "test")

    def test_nav(self):
        r = self.archive.portfolios.nav["test"]
        self.assertAlmostEqual(r[pd.Timestamp("2015-04-22")], 1.0070191775792583, places=5)

    def test_porfolio_none(self):
        p = self.archive.portfolios["abc"]
        assert not p

    def test_portfolio(self):
        d = {x: p for x, p in self.archive.portfolios.items()}
        self.assertListEqual(["test"], list(d.keys()))

    def test_sector_weights(self):
        symbolmap = self.archive.symbols.frame["group"]
        sector_w = self.archive.portfolios.sector_weights("test", symbolmap)
        self.assertAlmostEqual(sector_w["Equity"]["2013-01-04"], 0.24351702703439526, places=5)

    def test_portfolio(self):
        portfolio = test_portfolio()
        g = self.archive.portfolios["test"]

        pdt.assert_frame_equal(portfolio.prices, g.prices)
        pdt.assert_frame_equal(portfolio.weights, g.weights)

