from unittest import TestCase
from test.config import read_series

from pyutil.performance.var import value_at_risk, conditional_value_at_risk, VaR


class TestVar(TestCase):
    def test_var(self):
        s = read_series("ts.csv", parse_dates=True)
        x = 100*value_at_risk(s, alpha=0.99)
        self.assertAlmostEqual(x, 0.47550914363392316, places=10)

    def test_cvar(self):
        s = read_series("ts.csv", parse_dates=True)
        x = 100*conditional_value_at_risk(s, alpha=0.99)
        self.assertAlmostEqual(x, 0.51218385609772821, places=10)

    def test_class(self):
        s = read_series("ts.csv", parse_dates=True)
        v = VaR(s, alpha=0.99)
        self.assertAlmostEqual(100*v.cvar, 0.51218385609772821, places=10)
        self.assertAlmostEqual(100*v.var, 0.47550914363392316, places=10)