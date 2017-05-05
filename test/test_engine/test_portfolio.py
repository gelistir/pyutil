from unittest import TestCase

import pandas as pd
from mongoengine import connect

from pyutil.engine.portfolio import Strat, portfolio_builder, portfolio_names
from pyutil.portfolio.portfolio import Portfolio
from test.config import test_portfolio
import pandas.util.testing as pdt

portfolio = test_portfolio(group="A", comment="Peter Maffay", time=pd.Timestamp("now"))



class TestPortfolio(TestCase):
    @classmethod
    def setUpClass(cls):
        connect(db="testPortfolio", host="mongo", port=27017, alias="default")

        # Create a text-based post
        s1 = Strat(name="strat1", group="A", source="Peter Maffay", time=portfolio.meta["time"])
        s1.save()
        s1.update_portfolio(portfolio)

        s2 = Strat(name="strat2", group="A", source="Peter Maffay", time=portfolio.meta["time"])
        s2.save()
        s2.update_portfolio(portfolio)

    def test_names(self):
        self.assertSetEqual(portfolio_names(), {"strat1","strat2"})

    def test_count(self):
        assert Strat.objects.count()==2

    def test_builder(self):
        x = portfolio_builder("strat2")
        assert isinstance(x, Portfolio)

        p = test_portfolio()
        pdt.assert_frame_equal(p.prices, x.prices)
        pdt.assert_frame_equal(p.weights, x.weights)
    #     x = asset_builder(name="aaa")
    #     print(x)
    #     assert isinstance(x, Asset)