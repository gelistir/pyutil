from pyutil.timeseries.merge import merge, last_index, first_index, to_datetime, to_date
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

    def test_to_datetime(self):

        t0 = pd.Timestamp("2015-04-22")
        x = pd.Series(index=[t0], data=[2.0])

        self.assertIsInstance(x.index[0], pd.Timestamp)

        # should be safe to apply to_datetime
        pdt.assert_series_equal(x, to_datetime(x))

        y = pd.Series(index=[t0.date()], data=[2.0])
        pdt.assert_series_equal(y, to_date(x))

        self.assertIsNone(to_datetime(None))
        self.assertIsNone(to_date(None))

