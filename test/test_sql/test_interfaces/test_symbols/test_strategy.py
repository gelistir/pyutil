from unittest import TestCase

import pandas as pd

from pyutil.sql.interfaces.symbols.strategy import Strategy
from pyutil.sql.interfaces.symbols.symbol import Symbol
from test.config import resource

# Code in source.py
#
# import pandas as pd
#
# from test.config import test_portfolio
#
#
# class Configuration(object):
#     def __init__(self, reader):
#         self.__reader = reader
#         self.__portfolio = test_portfolio().truncate(before=pd.Timestamp("2015-04-01"))
#
#     @property
#     def portfolio(self):
#         return self.__portfolio
#
#     @property
#     def assets(self):
#         return self.__portfolio.assets


class TestStrategy(TestCase):
    def test_strategy(self):
        with open(resource("source.py"), "r") as f:
            s = Strategy(name="peter", source=f.read(), active=True)

            # this will just return the test_portfolio
            config = s.configuration(reader=None)
            portfolio = config.portfolio

            # This will return the assets as given by the reader!
            assets = config.assets
            self.assertListEqual(assets, ["A", "B", "C", "D", "E", "F", "G"])

            assets = {asset: Symbol(name=asset) for asset in assets}

            # store the portfolio we have just computed in there...
            s.upsert(portfolio, assets=assets)

            self.assertEqual(s.portfolio.last_valid_index(), pd.Timestamp("2015-04-22"))
            self.assertAlmostEqual(s.portfolio.weights[assets["G"]][pd.Timestamp("2015-04-17")], 0.04689036015357765)

            s.upsert(10*portfolio.tail(5), days=5, assets=assets)

            # observe the jump now...
            self.assertAlmostEqual(s.portfolio.weights[assets["G"]][pd.Timestamp("2015-04-17")], 0.4689036015357765)

