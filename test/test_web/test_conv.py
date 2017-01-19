from unittest import TestCase

from test.config import read_series
s = read_series("ts.csv")

from pyutil.web.conv import series2array

class TestRequestPandas(TestCase):
    def test_toArray(self):
        a = series2array(s)
        self.assertAlmostEqual(a[0][0], 1388530800000.0, places=5)
        self.assertAlmostEqual(a[0][1], 1.3063517821860473, places=10)



