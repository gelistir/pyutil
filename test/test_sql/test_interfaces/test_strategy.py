import pandas as pd
from unittest import TestCase

from pyutil.sql.interfaces.portfolio import Portfolio
from pyutil.sql.interfaces.symbol import Symbol
from pyutil.sql.interfaces.strategy import Strategy, StrategyType
from pyutil.sql.container import Assets
from test.config import resource, test_portfolio
import pandas.util.testing as pdt


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

    def test_upsert(self):
        s = Strategy(name="Maffay", active=True, source="", type=StrategyType.dynamic)
        p = Portfolio(name="test")
        assets = Assets([Symbol(bloomberg_symbol=asset) for asset in ["A", "B", "C", "D", "E", "F", "G"]])

        x = s.upsert(test_portfolio(), assets=assets.to_dict())
        pdt.assert_frame_equal(x.weights.rename(columns=lambda x: x.bloomberg_symbol), test_portfolio().weights)
        pdt.assert_frame_equal(x.prices.rename(columns=lambda x: x.bloomberg_symbol), test_portfolio().prices)