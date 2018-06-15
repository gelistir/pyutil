import pandas as pd
from unittest import TestCase

from pyutil.portfolio.portfolio import Portfolio, merge


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

        self.assertEqual(portfolio.last_valid_index, 2)
        self.assertEqual(str(portfolio), "Portfolio with assets: ['B', 'A']")

    def test_forward(self):
        prices = pd.DataFrame(columns=["A", "B"], index=[1,2,3], data=[[100,120],[110, 110],[130,120]])

        portfolio = Portfolio(prices = prices)

        portfolio.weights.loc[1] = {"A": 0.5, "B": 0.4}

        # forward the weights from the previous state
        portfolio.forward(2)
        portfolio.forward(3)

        self.assertAlmostEqual(portfolio.weights["A"][3], 0.56521739130434789, places=5)

    def test_empty(self):
        portfolio = Portfolio(prices = pd.DataFrame({}))
        self.assertIsNone(portfolio.last_valid_index)
        self.assertTrue(portfolio.empty)

    def test_merge(self):
        prices1 = pd.DataFrame(columns=["B", "A"], index=[1, 2], data=100)

        portfolio1 = Portfolio(prices=prices1)
        portfolio1.weights.loc[1] = {"A": 0.5, "B": 0.5}
        portfolio1.weights.loc[2] = {"A": 0.3, "B": 0.7}

        prices2 = pd.DataFrame(columns=["C", "D"], index=[1, 2], data=200)

        portfolio2 = Portfolio(prices=prices2)
        portfolio2.weights.loc[1] = {"C": 0.5, "D": 0.5}
        portfolio2.weights.loc[2] = {"C": 0.3, "D": 0.7}

        portfolio = merge(portfolios=[portfolio1, portfolio2], axis=1)

        self.assertListEqual(portfolio.assets, ["A","B","C","D"])


        prices3 = pd.DataFrame(columns=["A", "B"], index=[1, 2], data=200)

        portfolio3 = Portfolio(prices=prices3)
        portfolio3.weights.loc[1] = {"A": 0.5, "B": 0.5}
        portfolio3.weights.loc[2] = {"A": 0.3, "B": 0.7}

        with self.assertRaises(ValueError):
            # overlapping columns!
            merge(portfolios=[portfolio1, portfolio3], axis=1)

