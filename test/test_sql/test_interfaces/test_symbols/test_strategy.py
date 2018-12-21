import io
import tempfile

import pandas as pd

import pandas.util.testing as pdt
from unittest import TestCase
from test.config import test_portfolio, resource

from pyutil.sql.interfaces.symbols.strategy import Strategy
from pyutil.sql.interfaces.symbols.symbol import Symbol, SymbolType


class TestStrategy(TestCase):
    @classmethod
    def setUpClass(cls):
        with open(resource("source.py"), "r") as f:
            cls.strategy = Strategy(name="Peter", source=f.read(), active=True)

        # compute a portfolio
        portfolio = cls.strategy.configuration(reader=None).portfolio
        cls.assets = {name: Symbol(name=name, group=SymbolType.fixed_income) for name in portfolio.assets}

        # upsert the strategy
        cls.strategy.upsert(portfolio=portfolio, symbols=cls.assets)

    def test_upsert(self):
        self.assertEqual(self.strategy.last, pd.Timestamp("2015-04-22"))

        # extract the portfolio
        p = self.strategy.portfolio
        pdt.assert_frame_equal(p.weights, test_portfolio().weights, check_names=False)
        pdt.assert_frame_equal(p.prices, test_portfolio().prices, check_names=False)

        # upsert the last 10 days
        self.strategy.upsert(portfolio=5 * test_portfolio().tail(10), days=10, symbols=self.assets)

        # extract again the portfolio
        p = self.strategy.portfolio

        x = p.weights.tail(12).sum(axis=1)
        self.assertAlmostEqual(x["2015-04-08"], 0.305048, places=5)
        self.assertAlmostEqual(x["2015-04-13"], 1.486652, places=5)

        self.assertSetEqual(set(self.strategy.assets), set(self.assets.values()))

        a = self.strategy.to_json()
        self.assertEqual(a["name"], "Peter")

    def test_csv(self):
        # extract csv streams
        prices, weights = self.strategy.to_csv()

        # read streams back and compare
        pdt.assert_frame_equal(pd.read_csv(io.StringIO(prices), index_col=0, parse_dates=True), self.strategy.portfolio.prices)
        pdt.assert_frame_equal(pd.read_csv(io.StringIO(weights), index_col=0, parse_dates=True), self.strategy.portfolio.weights)

        test_dir = tempfile.mkdtemp()
        self.strategy.to_csv(folder=test_dir)
        self.strategy.read_csv(folder=test_dir, symbols=self.assets)


    def test_sector(self):
        print(self.strategy.sector())
        print(self.strategy.state)
        # todo: finish test
