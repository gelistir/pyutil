from unittest import TestCase

import pandas as pd

from pyutil.engine.portfolio import Strat, portfolios, from_portfolio
from test.config import test_portfolio, connect
import pandas.util.testing as pdt

portfolio = test_portfolio()


class TestPortfolio(TestCase):
    @classmethod
    def setUpClass(cls):
        connect()

        from_portfolio(name="strat1", portfolio=portfolio, group="A").save()
        from_portfolio(name="strat2", portfolio=portfolio, group="A").save()


    @classmethod
    def tearDownClass(cls):
        Strat.drop_collection()

    def test_count(self):
        assert Strat.objects.count() == 2

    def test_update(self):
        s = Strat.objects(name="strat1")[0]
        x = s.update_portfolio((2*portfolio).tail(10))
        pdt.assert_frame_equal(2*portfolio.weights.tail(10), x.portfolio.weights.tail(10))

    def test_portfolios(self):
        x = portfolios()
        self.assertSetEqual({"strat1","strat2"}, set(x.keys()))

        x = portfolios(names=["strat1"])
        self.assertEquals(len(x), 1)


