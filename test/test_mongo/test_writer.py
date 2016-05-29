import pandas as pd
import pandas.util.testing as pdt
from ming import create_datastore
from pyutil.mongo.reader import _ArchiveReader
from pyutil.mongo.writer import _ArchiveWriter
from pyutil.nav.nav import Nav
from test.config import read_frame, test_portfolio
from unittest import TestCase


class TestWriter(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.db = create_datastore("tmp")
        cls.reader = _ArchiveReader(cls.db)
        cls.writer = _ArchiveWriter(cls.db)

        # write assets into test database. Writing is slow!
        assets = read_frame("price.csv", parse_dates=True)

        for asset in assets:
            cls.writer.update_asset(asset, assets[asset])

        frame = read_frame("symbols.csv")
        cls.writer.update_symbols(frame)

        p = test_portfolio()
        cls.writer.update_portfolio("test", p, group="test")

    def test_nav(self):
        portfolio = test_portfolio()
        self.writer.update_portfolio("test", portfolio, "test", n=10, comment="Hello World")
        self.writer.update_rtn(portfolio.nav.series, "test")

        g = self.reader.read_nav(name="test").fee(0.5).monthly.series
        x = Nav(self.reader.portfolios.nav["test"].dropna()).fee(0.5).monthly.series

        self.assertAlmostEqual(g[pd.Timestamp("2015-04-30")], 0.97715910781949233, places=5)
        self.assertAlmostEqual(x[pd.Timestamp("2015-04-30")], 0.97715910781949233, places=5)

    def test_frame(self):
        self.writer.update_frame(name="Peter Maffay", frame=pd.DataFrame(columns=["A","B"], data=[[1.0, 2.0]]))
        x = self.reader.read_frame(name="Peter Maffay")
        self.assertListEqual(list(x.keys()), ["A", "B"])

        y = self.reader.read_frame()
        self.assertListEqual(list(y["Peter Maffay"].keys()), ["A", "B"])

    def test_portfolio(self):
        portfolio = test_portfolio()
        self.writer.update_symbols(frame=read_frame("symbols.csv", parse_dates=False))
        self.writer.update_portfolio("test", portfolio, "test-group")
        # self.archive.update_portfolio("test", portfolio, "test-group", n=10)
        g = self.reader.portfolios["test"]

        pdt.assert_frame_equal(portfolio.prices, g.prices)
        pdt.assert_frame_equal(portfolio.weights, g.weights)