from unittest import TestCase

import pandas as pd

from pyutil.engine.portfolio import Strat, portfolios
from pyutil.portfolio.portfolio import Portfolio
from test.config import test_portfolio, connect
import pandas.util.testing as pdt

portfolio = test_portfolio(group="A", comment="Peter Maffay", time=pd.Timestamp("now"))


class TestPortfolio(TestCase):
    @classmethod
    def setUpClass(cls):
        connect()

        # Create a text-based post
        s1 = Strat(name="strat1", group="A", source="Peter Maffay", time=portfolio.meta["time"])
        s1.save()
        s1.update_portfolio(portfolio)

        s2 = Strat(name="strat2", group="A", source="Peter Maffay", time=portfolio.meta["time"])
        s2.save()
        s2.update_portfolio(portfolio)

    @classmethod
    def tearDownClass(cls):
        Strat.drop_collection()

    def test_count(self):
        assert Strat.objects.count() == 2

    def test_update(self):
        s = Strat.objects(name="strat1")[0]
        x = s.update_portfolio((2*portfolio).tail(10))
        #print(x.portfolio.weights.tail(10))
        #print(portfolio.weights.tail(10))
        pdt.assert_frame_equal(2*portfolio.weights.tail(10), x.portfolio.weights.tail(10))

    def test_portfolios(self):
        x = portfolios()
        self.assertSetEqual({"strat1","strat2"}, set(x.keys()))

        x = portfolios(names=["strat1"])
        self.assertEquals(len(x), 1)


