from pyutil.timeseries.merge import merge, last_index, first_index
from unittest import TestCase
import pandas as pd
import pandas.util.testing as pdt

from test.config import read_series
s = read_series("ts.csv")

index = pd.Timestamp("2015-04-14")


class TestMerge(TestCase):
    def test_last_index(self):
        t = last_index(s)
        t0 = pd.Timestamp("2015-04-22")
        self.assertEqual(t, t0)
        self.assertIsNone(last_index(None))
        self.assertEqual(last_index(None, default=t0), t0)
        self.assertEqual(last_index(pd.Series({}), default=t0), t0)

    def test_first_index(self):
        t = first_index(s)
        t0 = pd.Timestamp("2014-01-01")
        self.assertEqual(t, t0)
        self.assertIsNone(first_index(None))
        self.assertEqual(first_index(None, default=t0), t0)
        self.assertEqual(first_index(pd.Series({}), default=t0), t0)

    def test_merge(self):
        x = merge(new=s)
        pdt.assert_series_equal(x, s)

        x = merge(new=s, old=s)
        pdt.assert_series_equal(x, s)

        x = merge(new=5*s, old=s)
        pdt.assert_series_equal(x, 5*s)

        y = merge(None)
        self.assertIsNone(y)

        y = merge(pd.Series({}), None)
        pdt.assert_series_equal(y, pd.Series({}))

        y = merge(pd.Series({}), pd.Series({}))
        pdt.assert_series_equal(y, pd.Series({}))
