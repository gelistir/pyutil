from unittest import TestCase

import pandas as pd
import pandas.util.testing as pdt

from pyutil.mongo.mongoArchive import MongoArchive
from test.config import read_frame, test_portfolio


prices = read_frame("price.csv", parse_dates=True)
symbols = read_frame("symbols.csv")
testp = test_portfolio(group="test", comment="test", time=pd.Timestamp("1980-01-01"))

class TestMongoArchive(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.archive = MongoArchive()
        cls.archive.symbols.update_all(symbols)
        cls.archive.time_series.update_all(prices)
        cls.archive.portfolios.update("test", testp)

    def test_history(self):
        pdt.assert_frame_equal(self.archive.history("PX_LAST")[["A","B"]], prices[["A", "B"]])

    #def test_symbols(self):
    #    pdt.assert_frame_equal(self.archive.assets().reference.sort_index(axis=1), symbols.sort_index(axis=1), check_dtype=False)

    def test_asset(self):
        a = self.archive.assets(names=["A"])
        self.assertEquals(a["A"].reference["internal"], "Gold")

    def test_get(self):
        p = self.archive.portfolios["test"]
        self.assertDictEqual(p.meta, {'comment': 'test', 'time': pd.Timestamp("01-01-1980"), 'group': 'test'})

    def test_porfolio_none(self):
        p = self.archive.portfolios["abc"]
        assert not p

    def test_update(self):
        portfolio = testp
        self.archive.portfolios.update(key="test", portfolio=portfolio.tail(10))

        g = self.archive.portfolios["test"]
        pdt.assert_frame_equal(portfolio.prices, g.prices)
        pdt.assert_frame_equal(portfolio.weights, g.weights)

    def test_history_entire(self):
        pdt.assert_frame_equal(self.archive.history("PX_LAST"), prices)

    def test_history_columns(self):
        pdt.assert_series_equal(self.archive.history("PX_LAST")["A"], prices["A"], check_names=False)


    def test_time_series(self):
        pdt.assert_series_equal(self.archive.time_series["A"]["PX_LAST"], prices["A"], check_names=False)

    def test_no_time_series(self):
        with self.assertRaises(KeyError):
            self.archive.time_series["Peter Maffay"]

    def test_wrong_field(self):
        with self.assertRaises(KeyError):
            self.archive.time_series["A"]["Peter Maffay"]

    def test_reference(self):
        print(self.archive.reference)
        pdt.assert_frame_equal(self.archive.reference, symbols.sort_index(axis=1), check_dtype=False)




