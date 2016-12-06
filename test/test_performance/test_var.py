from unittest import TestCase
from test.config import read_series

from pyutil.performance.var import value_at_risk, conditional_value_at_risk

class TestVar(TestCase):
    def test_var(self):
        s = read_series("ts.csv", parse_dates=True)
        x = 100*value_at_risk(s, alpha=0.99)
        self.assertAlmostEqual(x, 0.40086450047240874, places=10)

    def test_cvar(self):
        s = read_series("ts.csv", parse_dates=True)
        x = 100*conditional_value_at_risk(s, alpha=0.99)
        self.assertAlmostEqual(x, 0.53542831745811131, places=10)
