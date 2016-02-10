from pyutil.nav.nav import Nav
from unittest import TestCase
import pandas as pd

from test.config import read_series

nav = Nav(read_series("ts.csv", parse_dates=True))


class TestNav(TestCase):
    def test_drawdown(self):
        self.assertAlmostEqual(100 * nav.drawdown.max(), 5.7000575458488578, places=6)

    def test_performance(self):
        self.assertAlmostEqual(nav.performance(days=256)["Max Drawdown"], 5.7000575458488578, places=6)

    def test_monthly_table(self):
        self.assertAlmostEqual(100 * nav.monthlytable["Nov"][2013], 0.23233078558395626, places=5)

    def test_summary(self):
        self.assertAlmostEqual(nav.summary(days=256)[250]["Max Drawdown"], 3.9885756705666631, places=6)

    def test_ewm(self):
        self.assertAlmostEqual(100 * nav.ewm_volatility(days=250).values[-1], 2.7706672542422539, places=6)
        self.assertAlmostEqual(100 * nav.ewm_ret(days=250).values[-1], 6.0326401733122053, places=6)
        self.assertAlmostEqual(nav.ewm_sharpe(days=250).values[-1], 2.177323951144059, places=6)

    def test_adjust(self):
        x = Nav(pd.Series(data=[50, 60, 55]))
        y = x.adjust(100.0)#.series
        self.assertAlmostEqual(y[0], 100.0, places=4)
        self.assertAlmostEqual(y[1], 120.0, places=4)

    def test_truncate(self):
        x = nav.truncate(before=pd.Timestamp("2014-01-01"), after=pd.Timestamp("2014-02-28"))
        self.assertEqual(len(x.series), 43)

    def test_period_returns(self):
        x = 100 * nav.period_returns()
        self.assertAlmostEqual(x["Ten Years"], 23.072247025834592, places=5)

    def test_fee(self):
        x = nav.fee(0.5)
        self.assertAlmostEqual(x[x.series.index[-1]], 1.1175918337152901, places=5)
        x = nav.fee(0.0)
        self.assertAlmostEqual(x[x.series.index[-1]], 1.3215650061893029, places=5)

    def test_losses(self):
        print(nav.losses)
        self.assertAlmostEqual(nav.losses["2013-01-21"], -0.00045747477090229971, places=5)

    def test_monthly(self):
        self.assertAlmostEqual(nav.monthly[pd.Timestamp("2014-11-30")], 1.2935771592500624, places=5)

    def test_annual(self):
        self.assertAlmostEqual(nav.annual[pd.Timestamp("2014-12-31")], 1.2934720900884369, places=5)
