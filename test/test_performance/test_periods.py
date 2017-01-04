from unittest import TestCase

import pandas as pd

from test.config import read_series

from pyutil.performance.periods import periods, period_returns
import pandas.util.testing as pdt

class TestPeriods(TestCase):
    def test_periods(self):
        p = periods(today=pd.Timestamp("2015-05-01"))
        self.assertEqual(p["Two weeks"].start, pd.Timestamp("2015-04-17"))
        self.assertEqual(p["Two weeks"].end, pd.Timestamp("2015-05-01"))

    def test_period_returns(self):
        p = periods(today=pd.Timestamp("2015-05-01"))
        s = read_series("ts.csv", parse_dates=True).pct_change().dropna()
        x = 100*period_returns(returns=s, offset=p)
        self.assertAlmostEqual(x["Three Years"], 1.1645579858904798 , places=10)

    def test_periods_more(self):
        s = read_series("ts.csv", parse_dates=True).pct_change().dropna()
        y = period_returns(s, offset=periods(today=s.index[-1]))
        pdt.assert_series_equal(y, read_series("periods.csv", parse_dates=False), check_names=False)
