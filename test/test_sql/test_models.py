from unittest import TestCase

import pandas as pd

from pyutil.sql.interfaces.symbol import Symbol
from pyutil.sql.interfaces.portfolio import Portfolio
from pyutil.sql.interfaces.strategy import Strategy
from test.config import test_portfolio, resource


class TestModels(TestCase):

    def test_strategy(self):
        with open(resource("source.py"), "r") as f:
            s = Strategy(name="peter", source=f.read(), active=True)
            # No, this doesn't work!
            portfolio = s.compute_portfolio(reader=None)

            assets = {asset: Symbol(bloomberg_symbol=asset) for asset in portfolio.assets}

            p = Portfolio(name="test", strategy=s)
            #print(portfolio)

            p.upsert(portfolio, assets=assets)

            self.assertEqual(s._portfolio.last_valid, pd.Timestamp("2015-04-22"))

