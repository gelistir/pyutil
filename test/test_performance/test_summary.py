from unittest import TestCase
import pandas as pd

from pyutil.performance.summary import NavSeries, performance, fromNav
from pyutil.timeseries.timeseries import adjust
from test.config import read_series

import pandas.util.testing as pdt
s = NavSeries(read_series("ts.csv", parse_dates=True))


class TestSummary(TestCase):
    def test_pos_neg(self):
        self.assertEqual(s.negative_events, 164)
        self.assertEqual(s.positive_events, 176)
        self.assertEqual(s.events, 340)

    def test_summary(self):
        pdt.assert_series_equal(s.summary().apply(str), read_series("summary.csv").apply(str), check_names=False)

    def test_autocorrelation(self):
        self.assertAlmostEqual(s.autocorrelation, 0.070961153249184269, places=10)

    def test_mtd(self):
        self.assertAlmostEqual(100*s.mtd, 1.4133604922211385, places=10)

    def test_ytd(self):
        self.assertAlmostEqual(100*s.ytd, 2.1718996734564122, places=10)

    def test_monthly_table(self):
        self.assertAlmostEqual(100 * s.monthlytable["Nov"][2014], -0.19540358586001005, places=5)

    def test_ewm(self):
        self.assertAlmostEqual(100 * s.ewm_volatility(periods=250).values[-1], 2.7714298334400818, places=6)
        self.assertAlmostEqual(100 * s.ewm_ret(periods=250).values[-1], 6.0365130705403685, places=6)
        self.assertAlmostEqual(s.ewm_sharpe(periods=250).values[-1], 2.1781222810347862, places=6)

    def test_performance(self):
        result = performance(s)
        self.assertAlmostEqual(result["Max Drawdown"], 3.9885756705666631, places=10)

    def test_fee(self):
        x = s.fee(0.5)
        self.assertAlmostEqual(x[x.index[-1]], 0.99454336215760819, places=5)
        x = s.fee(0.0)
        self.assertAlmostEqual(x[x.index[-1]], 1.0116455798589048, places=5)

    def test_monthly(self):
        self.assertAlmostEqual(s.monthly[pd.Timestamp("2014-11-30")], 1.2935771592500624, places=5)

    def test_annual(self):
        self.assertAlmostEqual(s.annual[pd.Timestamp("2014-12-31")], 1.2934720900884369, places=5)

    def test_weekly(self):
        self.assertEqual(len(s.weekly.index), 70)
        #print(s.daily)
        #self.assertEqual(len(s.daily.index), 477)

    def test_annual_returns(self):
        aaa = adjust(s)
        #print(aaa.truncate(after=pd.Timestamp("2015-01-01")))

        xx = s.annual_returns
        print(xx)

    def test_truncate(self):
        x = s.truncate(before="2015-01-01")
        self.assertEqual(x.index[0], pd.Timestamp("2015-01-01"))

    def test_fromNav(self):
        x = fromNav(ts=read_series("ts.csv", parse_dates=True))
        self.assertAlmostEqual(x.autocorrelation, 0.070961153249184269, places=10)

    def test_periods(self):
        p=s.period_returns
        self.assertAlmostEqual(p.loc["Three Years"], 0.011645579858904798, places=10)

    def test_drawdown_periods(self):
        p = s.drawdown_periods
        self.assertEqual(p.loc[pd.Timestamp("2014-03-07").date()], pd.Timedelta(days=63))
