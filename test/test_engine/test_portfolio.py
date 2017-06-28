from unittest import TestCase

from pyutil.engine.aux import frame2dict
from pyutil.engine.portfolio import Strat, portfolios, portfolio
from test.config import test_portfolio, connect
import pandas.util.testing as pdt

p = test_portfolio()


class TestPortfolio(TestCase):
    @classmethod
    def setUpClass(cls):
        connect()
        portfolio(name="strat1").update(weights=frame2dict(p.weights), prices=frame2dict(p.prices), group="A")
        portfolio(name="strat2").update(weights=frame2dict(p.weights), prices=frame2dict(p.prices), group="A")

    @classmethod
    def tearDownClass(cls):
        Strat.drop_collection()

    def test_count(self):
        assert Strat.objects.count() == 2

    def test_update(self):
        s = portfolio(name="strat1")
        x = s.update_portfolio((2 * p).tail(10))
        pdt.assert_frame_equal(2 * p.weights.tail(10), x.portfolio.weights.tail(10))

    def test_portfolios(self):
        x = portfolios()
        self.assertSetEqual({"strat1","strat2"}, set(x.keys()))

        x = portfolios(names=["strat1"])
        self.assertEquals(x.len(), 1)


