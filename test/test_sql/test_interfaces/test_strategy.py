import pandas as pd
from unittest import TestCase

from pyutil.sql.interfaces.portfolio import Portfolio
from pyutil.sql.interfaces.symbol import Symbol
from pyutil.sql.interfaces.strategy import Strategy
from test.config import resource


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

            assets = {asset: Symbol(bloomberg_symbol=asset) for asset in assets}

            # make a new SQL object
            p = Portfolio(name="test", strategy=s)

            # store the portfolio we have just computed in there...
            p.upsert(portfolio, assets=assets)

            self.assertEqual(s.portfolio.last_valid_index(), pd.Timestamp("2015-04-22"))
            p.upsert(portfolio.tail(5), assets=assets)

            s.upsert(3*portfolio.tail(5), days=3, assets=assets)


            print(p.portfolio.weights.tail(10))

