import pandas as pd
from pyutil.mongo.reader import ArchiveReader
from pyutil.mongo.archive import database
from pyutil.mongo.writer import ArchiveWriter
from unittest import TestCase

from test.config import read_frame, test_portfolio, test_database

import pandas.util.testing as pdt

class TestWriter(TestCase):
    @classmethod
    def setUpClass(cls):

        cls.db = test_database()
        cls.writer = ArchiveWriter(cls.db)
        cls.reader = ArchiveReader(cls.db)

        # write assets into test database. Writing is slow!
        assets = read_frame("price.csv", parse_dates=True)

        for asset in assets:
            cls.writer.update_asset(asset, assets[asset])

        frame = read_frame("symbols.csv")
        cls.writer.update_symbols(frame)

        p = test_portfolio()
        cls.writer.update_portfolio("test", p, group="test")

    @classmethod
    def tearDownClass(cls):
        cls.db.client.drop_database(cls.db)

    def test_nav(self):
        portfolio = test_portfolio()
        self.writer.update_rtn(portfolio.nav.fee(0.5).series, "test", fee=0.5)
        g = self.reader.read_nav("M", name="test", fee=0.5).series

        self.assertAlmostEqual(g[pd.Timestamp("2015-04-30")], 1.0133233120470464, places=5)

    def test_free(self):
        self.writer.update_dict(name="Peter Maffay", data=["A", "B", "C"])
        x = self.reader.read_free()
        self.assertListEqual(x["Peter Maffay"], ["A", "B", "C"])

    def test_portfolio(self):
        portfolio = test_portfolio()
        self.writer.update_symbols(frame=read_frame("symbols.csv", parse_dates=False))
        self.writer.update_portfolio("test", portfolio, "test-group")
        # self.archive.update_portfolio("test", portfolio, "test-group", n=10)
        g = self.reader.portfolios["test"]

        pdt.assert_frame_equal(portfolio.prices, g.prices)
        pdt.assert_frame_equal(portfolio.weights, g.weights)