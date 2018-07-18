import pandas.util.testing as pdt
from unittest import TestCase

from pyutil.influx.client_test import init_influxdb
from pyutil.sql.interfaces.symbols.strategy import Strategy
from test.config import test_portfolio, resource


class TestStrategy(TestCase):
    @classmethod
    def setUpClass(cls):
        init_influxdb()

        with open(resource("source.py"), "r") as f:
            cls.s = Strategy(name="Peter", source=f.read(), active=True)

        # this is a way to compute a portfolio from the source code given in source.py
        config = cls.s.configuration(reader=None)
        portfolio = config.portfolio

        cls.s.upsert(portfolio=portfolio)


    def test_upsert(self):
        p = self.s.portfolio
        pdt.assert_frame_equal(p.weights, test_portfolio().weights, check_names=False)
        pdt.assert_frame_equal(p.prices, test_portfolio().prices, check_names=False)

        self.s.upsert(portfolio=5*test_portfolio().tail(10), days=10)
        p = self.s.portfolio
        x = p.weights.tail(12).sum(axis=1)
        self.assertAlmostEqual(x["2015-04-08"], 0.305048, places=5)
        self.assertAlmostEqual(x["2015-04-13"], 1.486652, places=5)

