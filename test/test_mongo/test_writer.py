from unittest import TestCase

import pandas as pd
import pandas.util.testing as pdt

from pyutil.mongo.archive import writer, reader
from test.config import read_frame, test_portfolio


class TestWriter(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.writer = writer("tmp_J32JFASDK", host="mongo")
        cls.reader = reader("tmp_J32JFASDK", host="mongo")

        # write assets into test database. Writing is slow!
        cls.writer.update_assets(frame=read_frame("price.csv", parse_dates=True))
        cls.writer.update_symbols(frame=read_frame("symbols.csv"))
        cls.writer.update_portfolio("test", test_portfolio(), group="test")

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
