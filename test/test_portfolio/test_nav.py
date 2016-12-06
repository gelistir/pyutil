from unittest import TestCase

import pandas as pd

from pyutil.portfolio.nav import fromNav
from test.config import read_series

nav = fromNav(read_series("ts.csv", parse_dates=True))


class TestNav(TestCase):
    def test_summary(self):
        s = nav.statistics.summary()
        self.assertAlmostEqual(s["Max Drawdown"], 5.7000575458488578 , places=10)

    def test_truncate(self):
        x = nav.truncate(before=pd.Timestamp("2014-01-01"), after=pd.Timestamp("2014-02-28"))
        self.assertEqual(len(x.series), 43)

    def test_fee(self):
        x = nav.fee(0.5)
        self.assertAlmostEqual(x[x.series.index[-1]], 1.1175918337152901, places=5)
        x = nav.fee(0.0)
        self.assertAlmostEqual(x[x.series.index[-1]], 1.3215650061893029, places=5)

    def test_monthly(self):
        self.assertAlmostEqual(nav.monthly[pd.Timestamp("2014-11-30")], 1.2935771592500624, places=5)

    def test_annual(self):
        self.assertAlmostEqual(nav.annual[pd.Timestamp("2014-12-31")], 1.2934720900884369, places=5)

    def test_weekly(self):
        self.assertEqual(len(nav.weekly.index), 671)

    def test_daily(self):
        self.assertEqual(len(nav.daily.index), 4693)

    def test_ytd(self):
        x = nav.ytd.adjust(100)

    def test_mtd(self):
        x = nav.mtd.adjust(100)

    def test_adjust(self):
        pass

    def test_drawdown(self):
        pass



