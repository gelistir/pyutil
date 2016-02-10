from unittest import TestCase
import pandas as pd

from pyutil.performance.drawdown import drawdown
from pyutil.performance.periods import period_returns, periods
from pyutil.performance.var import value_at_risk, conditional_value_at_risk
from pyutil.performance.month import monthlytable
from pyutil.performance.performance import performance

from test.config import read_series, read_frame

ts = read_series("ts.csv", parse_dates=True)

import pandas.util.testing as pdt


class TestPerformance(TestCase):
    def test_periods(self):
        y = period_returns(ts.pct_change(), offset=periods(ts.index[-1]))
        pdt.assert_series_equal(y, read_series("periods.csv", parse_dates=False), check_names=False)

    def test_drawdown(self):
        pdt.assert_series_equal(drawdown(ts), read_series("drawdown.csv", parse_dates=True), check_names=False)

    def test_var(self):
        var = value_at_risk(ts)
        cvar = conditional_value_at_risk(ts)

        self.assertAlmostEqual(var, 0.0040086450047240874, places=5)
        self.assertAlmostEqual(cvar, 0.0053542831745811131, places=5)

    def test_monthlytable(self):
        f = 100 * monthlytable(ts)
        f = f[f.index >= 2008]
        pdt.assert_frame_equal(f, read_frame("month.csv", parse_dates=False))

    def test_performance(self):
        p = performance(ts, 262)
        g = read_series("performance_a.csv", parse_dates=False)

        for i in p.index[:-2]:
            self.assertAlmostEqual(float(p.ix[i]), float(g.ix[i]), places=10)

        for i in p.index[-2:]:
            self.assertEqual(p.ix[i], pd.Timestamp(g.ix[i]))