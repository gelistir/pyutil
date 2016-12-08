import pandas as pd
from pyutil.portfolio.leverage import Leverage

from test.config import test_portfolio
from unittest import TestCase

l = Leverage(ts=test_portfolio().weights.sum(axis=1).dropna())

class TestLeverage(TestCase):
    def test_leverage(self):
        self.assertAlmostEqual(l[pd.Timestamp("2013-07-19")], 0.25505730106555635, places=5)

    def test_summary(self):
        x = l.summary()
        self.assertAlmostEqual(x["Av Leverage"], 0.35764570116711358, places=10)
        self.assertAlmostEqual(x["Current Leverage"], 0.3089738755134192, places=10)

    def test_truncate(self):
        x = l.truncate(before=pd.Timestamp("2015-01-01"))
        self.assertAlmostEqual(x.series.index[0], pd.Timestamp("2015-01-01"))
