from unittest import TestCase

import pandas as pd

from pyutil.engine.aux import frame2dict
from pyutil.engine.portfolio import Strat, portfolios
from test.config import test_portfolio, connect
import pandas.util.testing as pdt

portfolio = test_portfolio()


class TestPortfolio(TestCase):
    @classmethod
    def setUpClass(cls):

        def from_portfolio(portfolio, name, group, time=pd.Timestamp("now"), source=""):
            return Strat(name=name, weights=frame2dict(portfolio.weights), prices=frame2dict(portfolio.prices),
                         group=group, time=time, source=source)


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
        self.assertEquals(x.len(), 1)


