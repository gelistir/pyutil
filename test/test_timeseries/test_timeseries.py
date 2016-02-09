from unittest import TestCase
from test.config import read_series

nav = read_series("nav.csv", parse_dates=True)

from pyutil.timeseries.timeseries import subsample, adjust, ytd, mtd
import pandas as pd
import pandas.util.testing as pdt


class TestTimeseries(TestCase):
    def test_subsample_index(self):
        b = subsample(nav, day=15, incl=True)
        print(b)
        self.assertAlmostEqual(b[pd.Timestamp("2015-01-15")], 1.3028672967110184, places=5)

    def test_subsample_index2(self):
        b = subsample(nav, day=15, incl=True)
        self.assertEqual(len(b.index),3)

    def test_subsample_quarter(self):
        b = subsample(nav, day=15, incl=True).index[0:-1:1]
        self.assertEqual(b[1], pd.Timestamp("2015-01-15"))

    def test_adjust(self):
        x = adjust(nav.truncate(before="2015-01-01"))
        self.assertEqual(x.ix[x.index[0]], 1.0)

    def test_ytd(self):
        a = ytd(nav, today=pd.Timestamp("2014-05-07"))
        pdt.assert_series_equal(a, nav.truncate(before=pd.Timestamp("2014-01-01")))

    def test_mtd(self):
        a = mtd(nav, today=pd.Timestamp("2015-02-10"))
        print(a)
        pdt.assert_series_equal(a, nav.truncate(before=pd.Timestamp("2015-02-01")))
