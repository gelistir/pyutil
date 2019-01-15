from unittest import TestCase
import pandas as pd
import numpy as np

from pyutil.performance.summary import performance, fromNav, fromReturns
#from test.config import read_series

import pandas.util.testing as pdt

from pyutil.test.aux import read_series
from test.config import resource

ts = read_series(resource("ts.csv"), parse_dates=True)
s = fromNav(ts, adjust=True)


class TestSummary(TestCase):
    def test_adjust(self):
        x = s.adjust(value=10000)
        self.assertAlmostEqual(x.loc[0], 10000.0, places=10)

        y = 10000*fromNav(s, adjust=True)
        pdt.assert_series_equal(x, y)

    def test_summary(self):
        print(s.summary().apply(str).to_csv())
        pdt.assert_series_equal(s.summary().apply(str), read_series(resource("summary.csv")).apply(str), check_names=False, check_exact=False)
        x = fromNav(pd.Series(index=[pd.Timestamp("2017-01-04"), pd.Timestamp("2017-02-06")], data=[1.0, 1.02]))
        self.assertAlmostEqual(float(x.summary()["Annua Return"]), 22.0, places=10)

    def test_mtd(self):
        self.assertAlmostEqual(100 * s.mtd, 1.4133604922211385, places=10)
        x = pd.Series(index=[pd.Timestamp("2017-01-04"), pd.Timestamp("2017-01-06")], data=[1.0, 1.6])
        self.assertAlmostEqual(fromNav(x).mtd, 0.6, places=10)

    def test_ytd(self):
        self.assertAlmostEqual(100 * s.ytd, 2.1718996734564122, places=10)
        x = pd.Series(index=[pd.Timestamp("2017-01-04"), pd.Timestamp("2017-03-06")], data=[1.0, 1.6])
        self.assertAlmostEqual(fromNav(x).mtd, 0.6, places=10)
        self.assertAlmostEqual(fromNav(x).ytd, 0.6, places=10)

    def test_monthly_table(self):
        self.assertAlmostEqual(100 * s.monthlytable["Nov"][2014], -0.19540358586001005, places=5)

    def test_performance(self):
        result = performance(s)
        self.assertAlmostEqual(result["Max Drawdown"], 3.9885756705666631, places=10)

    def test_fee(self):
        x = s.fee(0.5)
        self.assertAlmostEqual(x[x.index[-1]], 0.99454336215760819, places=5)
        x = s.fee(0.0)
        self.assertAlmostEqual(x[x.index[-1]], 1.0116455798589048, places=5)

    def test_monthly(self):
        self.assertAlmostEqual(s.monthly[pd.Timestamp("2014-11-30")], 0.9902211463174124, places=5)

    def test_annual(self):
        self.assertAlmostEqual(s.annual[pd.Timestamp("2014-12-31")], 0.9901407168626069, places=5)

    def test_weekly(self):
        self.assertEqual(len(s.weekly.index), 70)
        # print(s.daily)
        # self.assertEqual(len(s.daily.index), 477)

    def test_annual_returns(self):
        x = s.returns_annual
        self.assertAlmostEqual(x[2014], -0.009859283137393149)
        self.assertAlmostEqual(x[2015],  0.021718996734564122)

    def test_truncate(self):
        x = s.truncate(before="2015-01-01")
        self.assertEqual(x.index[0], pd.Timestamp("2015-01-01"))

    def test_fromNav(self):
        x = fromNav(ts=read_series(resource("ts.csv"), parse_dates=True))
        pdt.assert_series_equal(x.series, read_series(resource("ts.csv"), parse_dates=True))

        x = fromNav(ts=None)
        pdt.assert_series_equal(x.series, pd.Series({}))

        with self.assertRaises(AssertionError):
            # you can't set a negative Nav value:
            fromNav(ts = pd.Series(data=[1,2,-10]))

    def test_periods(self):
        p = s.period_returns
        self.assertAlmostEqual(p.loc["Three Years"], 0.011645579858904798, places=10)

    def test_drawdown_periods(self):
        p = s.drawdown_periods
        self.assertEqual(p.loc[pd.Timestamp("2014-03-07").date()], pd.Timedelta(days=66))

    def test_with_dates(self):
        a = pd.Series({pd.Timestamp("2010-01-05").date(): 2.0,
                       pd.Timestamp("2012-02-13").date(): 3.0,
                       pd.Timestamp("2012-02-14").date(): 4.0
                       })

        n = fromNav(a)

        # no return in Jan 2012, 100% in Feb (from 2.0 to 4.0)
        pdt.assert_series_equal(n.ytd_series, pd.Series({"02": 1.0}))

        # we made 100% in Feb
        self.assertEqual(n.mtd, 1.0)
        self.assertEqual(n.ytd, 1.0)

    def test_adjust(self):
        n = fromNav(pd.Series({}))
        self.assertTrue(n.adjust().empty)

    def test_sortino_ratio_no_drawdown(self):
        x = pd.Series({pd.Timestamp("2012-02-13"): 1.0, pd.Timestamp("2012-02-14"): 1.0})
        n = fromNav(x)

        self.assertEqual(n.sortino_ratio(), np.inf)

    def test_recent(self):
        pdt.assert_series_equal(s.recent(2), s.pct_change().tail(2))

    def test_short(self):
        n = fromNav(ts=pd.Series({pd.Timestamp("30-Nov-2016"): 112}))
        self.assertEqual(n.periods_per_year, 256)

    def test_from_returns(self):
        x = pd.Series(data=[0.0, 0.1, -0.1])
        r = fromReturns(x, adjust=True)
        pdt.assert_series_equal(r.series, pd.Series([1.0, 1.1, 0.99]))

        r = fromReturns(None, adjust=True)
        pdt.assert_series_equal(r.series, pd.Series({}))
