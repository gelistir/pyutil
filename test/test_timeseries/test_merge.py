from pyutil.timeseries.merge import merge, last_index
from unittest import TestCase
import pandas as pd
import pandas.util.testing as pdt

from test.config import read_series
s = read_series("ts.csv")

index = pd.Timestamp("2015-04-14")


class TestMerge(TestCase):
    def test_last_index(self):
        t = last_index(s)
        self.assertEqual(t, pd.Timestamp("2015-04-22"))
        self.assertIsNone(last_index(None))
        self.assertEqual(last_index(None, default=pd.Timestamp("2015-04-22")), pd.Timestamp("2015-04-22"))

    def test_merge(self):
        x = merge(new=s)
        pdt.assert_series_equal(x, s)

        x = merge(new=s, old=s)
        pdt.assert_series_equal(x, s)

        x = merge(new=5*s, old=s)
        pdt.assert_series_equal(x, 5*s)
