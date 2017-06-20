import pandas as pd
from unittest import TestCase

from pyutil.portfolio.portfolio import Portfolio


class TestPortfolioBuilder(TestCase):

    def test_builder(self):
        prices = pd.DataFrame(columns=["B", "A"], index=[1, 2], data=100)

        portfolio = Portfolio(prices=prices)
        portfolio.weights.loc[1] = {"A": 0.5, "B": 0.5}
        portfolio.weights.loc[2] = {"A": 0.3, "B": 0.7}

        self.assertEqual(portfolio.prices["A"][2], 100)
        self.assertEqual(portfolio.asset_returns["A"][2], 0.0)
        self.assertEqual(portfolio.weights["A"][1], 0.5)
        self.assertEqual(portfolio.cash[2], 0.0)

    def test_forward(self):
        prices = pd.DataFrame(columns=["A", "B"], index=[1,2,3], data=[[100,120],[110, 110],[130,120]])

        portfolio = Portfolio(prices = prices)

        portfolio.weights.loc[1] = {"A": 0.5, "B": 0.4}

        # forward the weights from the previous state
        portfolio.forward(2)
        portfolio.forward(3)

        self.assertAlmostEqual(portfolio.weights["A"][3], 0.56521739130434789, places=5)

