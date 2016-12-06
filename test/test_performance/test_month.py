from unittest import TestCase
from test.config import read_series

from pyutil.performance.month import monthlytable

class TestMonth(TestCase):
    def test_table(self):
        s = read_series("ts.csv", parse_dates=True)
        x = 100*monthlytable(s)
        self.assertAlmostEqual(x["Mar"][2010], 0.75662665092133263, places=10)
        self.assertAlmostEqual(x["YTD"][2003], 5.1535333219787649, places=10)
        self.assertAlmostEqual(x["STDev"][2007], 2.69270123453546, places=10)
