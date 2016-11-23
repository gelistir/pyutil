import pandas as pd
from pyutil.mongo.archive import reader
from test.config import read_frame, test_portfolio
from unittest import TestCase
import pandas.util.testing as pdt

class TestArchive(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.archive = reader("tmp_JKJFDAFJJKFD", host="mongo")


        cls.archive.assets.update_all(frame=read_frame("price.csv", parse_dates=True))
        cls.archive.symbols.update_all(frame=read_frame("symbols.csv"))

        cls.archive.portfolios.update("test", test_portfolio(), group="test", comment="test")

        cls.archive.frames["Peter Maffay"] = pd.DataFrame(columns=["A", "B"], data=[[1.2, 2.5]])

    def test_history(self):
        a = self.archive.history(name="PX_LAST")
        self.assertAlmostEqual(a["B"][pd.Timestamp("2014-07-18").date()], 23454.79, places=5)

    def test_history_series(self):
        a = self.archive.history_series(item="B", name="PX_LAST")
        self.assertAlmostEqual(a[pd.Timestamp("2014-07-18")], 23454.79, places=5)

    def test_unknown_series(self):
        self.assertRaises(AssertionError, self.archive.history_series, item="XYZ", name="PX_LAST")

    def test_close(self):
        x = self.archive.history(items=["A", "B"], name="PX_LAST")
        self.assertAlmostEqual(x["B"][pd.Timestamp("2014-01-14")], 22791.28, places=5)

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

    def test_unknown_asset(self):
        self.assertRaises(AssertionError, self.archive.history, name="PX_LAST", items=["XYZ"])

    def test_unknown_series(self):
        self.assertRaises(AssertionError, self.archive.history, name="XYZ", items=["A", "B"])

    def test_sector_weights(self):
        symbolmap = self.archive.symbols.frame["group"]
        sector_w = self.archive.portfolios.sector_weights("test", symbolmap)
        self.assertAlmostEqual(sector_w["Equity"]["2013-01-04"], 0.24351702703439526, places=5)

    def test_frame(self):
        x = self.archive.frames["Peter Maffay"]
        pdt.assert_frame_equal(x, pd.DataFrame(columns=["A", "B"], data=[[1.2, 2.5]]))

        assert "Peter Maffay" in self.archive.frames.keys()
        assert len(self.archive.frames.keys())==1

        pair = self.archive.frames.items()[0]
        self.assertEqual(pair[0], "Peter Maffay")


    def test_portfolio(self):
        portfolio = test_portfolio()
        g = self.archive.portfolios["test"]

        pdt.assert_frame_equal(portfolio.prices, g.prices)
        pdt.assert_frame_equal(portfolio.weights, g.weights)

    def test_prices(self):
        a = self.archive.history(name="PX_LAST")
        self.assertAlmostEqual(a["B"][pd.Timestamp("2014-07-18").date()], 23454.79, places=5)

    def test_symbols(self):
        s = self.archive.symbols.frame
        self.assertEqual(s["group"]["A"], "Alternatives")

    def test_symbol(self):
        s = self.archive.symbols["A"]
        self.assertEqual(s["group"], "Alternatives")

