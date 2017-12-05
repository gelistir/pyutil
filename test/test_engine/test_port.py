from unittest import TestCase

import pandas.util.testing as pdt

from pyutil.engine.frame import Frame
from pyutil.engine.portfolio import upsert_portfolio, load_portfolio, portfolios
from pyutil.mongo.connect import connect_mongo
from pyutil.mongo.portfolios import Portfolios
from test.config import test_portfolio


class TestPort(TestCase):
    @classmethod
    def setUpClass(cls):
        connect_mongo('portfolios', host="testmongo", alias="default")
        p1 = test_portfolio()
        upsert_portfolio(name="hans", portfolio=p1)
        upsert_portfolio(name="panzer", portfolio=p1)

    def test_load_portfolio(self):
        p1 = load_portfolio(name="panzer")
        pdt.assert_frame_equal(test_portfolio().prices, p1.prices)
        pdt.assert_frame_equal(test_portfolio().weights, p1.weights)

    def test_portfolios(self):
        x = portfolios()
        self.assertTrue(type(x), Portfolios)
        self.assertSetEqual(set(x.keys()), {"hans", "panzer"})

        p1 = x["panzer"]
        pdt.assert_frame_equal(test_portfolio().prices, p1.prices)
        pdt.assert_frame_equal(test_portfolio().weights, p1.weights)

    def test_update(self):
        p1 = test_portfolio()
        upsert_portfolio(name="hans", portfolio= 2*p1.tail(5))
        p2 = load_portfolio(name="hans")
        self.assertAlmostEqual(p2.tail(5).weights.sum(axis=1).sum(), 2*p1.tail(5).weights.sum(axis=1).sum(), places=8)

    @classmethod
    def tearDownClass(cls):
        Frame.drop_collection()

