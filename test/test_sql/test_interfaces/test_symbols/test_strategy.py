import pandas as pd

import pandas.util.testing as pdt
from unittest import TestCase
from test.config import test_portfolio, resource

from pyutil.sql.interfaces.symbols.strategy import Strategy
from pyutil.sql.interfaces.symbols.symbol import Symbol


class TestStrategy(TestCase):
    @classmethod
    def setUpClass(cls):
        with open(resource("source.py"), "r") as f:
            cls.strategy = Strategy(name="Peter", source=f.read(), active=True)

    def test_upsert(self):
        self.assertIsNone(self.strategy.last)

        # compute a portfolio
        portfolio = self.strategy.configuration(reader=None).portfolio
        assets = {name: Symbol(name=name) for name in portfolio.assets}

        # upsert the strategy
        self.strategy.upsert(portfolio=portfolio, symbols=assets)
        self.assertEqual(self.strategy.last, pd.Timestamp("2015-04-22"))

        # extract the portfolio
        p = self.strategy.portfolio

        pdt.assert_frame_equal(p.weights, test_portfolio().weights, check_names=False)
        pdt.assert_frame_equal(p.prices, test_portfolio().prices, check_names=False)

        # upsert the last 10 days
        self.strategy.upsert(portfolio=5 * test_portfolio().tail(10), days=10, symbols=assets)

        # extract again the portfolio
        p = self.strategy.portfolio

        x = p.weights.tail(12).sum(axis=1)
        self.assertAlmostEqual(x["2015-04-08"], 0.305048, places=5)
        self.assertAlmostEqual(x["2015-04-13"], 1.486652, places=5)

        self.assertSetEqual(set(self.strategy.assets), set(assets.values()))
