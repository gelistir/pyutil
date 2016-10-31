import pandas as pd
from unittest import TestCase

from pyutil.portfolio.builder import PortfolioBuilder


class TestPortfolioBuilder(TestCase):

    def test_builder(self):
        prices = pd.DataFrame(columns=["A", "B"], index=[1, 2], data=100)

        builder = PortfolioBuilder(prices=prices)
        builder.weights.ix[1] = pd.Series({"A": 0.5, "B": 0.5})
        builder.weights.ix[2] = pd.Series({"A": 0.3, "B": 0.7})
        portfolio = builder.build()

        self.assertEqual(builder.weights["A"][1], 0.5)
        self.assertEqual(builder.assets, ["A", "B"])
        self.assertEqual(builder.prices["A"][2], 100)
        self.assertEqual(builder.returns["A"][2], 0.0)
        self.assertEqual(portfolio.weights["A"][1], 0.5)

