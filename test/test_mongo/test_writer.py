import pandas as pd
import pandas.util.testing as pdt
from pyutil.mongo.archive import writer, reader
from pyutil.mongo.writer import _flatten, _series2dict
from pyutil.nav.nav import Nav
from test.config import read_frame, test_portfolio
from unittest import TestCase


class TestWriter(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.writer = writer("tmp_J32JFASDK", host="mongo")
        cls.reader = reader("tmp_J32JFASDK", host="mongo")

        # write assets into test database. Writing is slow!
        cls.writer.update_assets(frame=read_frame("price.csv", parse_dates=True))
        cls.writer.update_symbols(frame=read_frame("symbols.csv"))
        cls.writer.update_portfolio("test", test_portfolio(), group="test")

    def test_nav(self):
        portfolio = test_portfolio()
        self.writer.update_portfolio("test", portfolio, "test", n=10, comment="Hello World")
        self.writer.update_rtn(portfolio.nav.returns, "test")

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

    def test_series2dict(self):
        x = pd.Series(index=[pd.Timestamp("2014-01-01"), pd.Timestamp("2014-02-26")], data=[1.0, 3.0])
        self.assertDictEqual(_series2dict(x), {'20140101': 1.0, '20140226': 3.0})

    def test_flatten_series(self):
        x = pd.Series(index=[pd.Timestamp("2014-01-01"), pd.Timestamp("2014-02-26")], data=[1.0, 3.0])
        self.assertDictEqual(_flatten("test", x), {'$set': {'test.20140226': 3.0, 'test.20140101': 1.0}})

    def test_flatten_frame(self):
        x = pd.Series(index=[pd.Timestamp("2014-01-01"), pd.Timestamp("2014-02-26")], data=[1.0, 3.0])
        frame = pd.DataFrame({"peter": x})
        self.assertDictEqual(_flatten("maffay", frame), {'$set': {'maffay.peter.20140226': 3.0, 'maffay.peter.20140101': 1.0}})
