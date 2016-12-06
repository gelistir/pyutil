from unittest import TestCase

from pyutil.performance.summary import Summary
from test.config import read_series

s = Summary(read_series("ts.csv", parse_dates=True))


class TestSummary(TestCase):
    def test_events(self):
        self.assertEqual(s.periods_per_year, 261)

    def test_first_last(self):
        self.assertEqual(s.first, 1.0)
        self.assertEqual(s.last, 1.3215650061893174)

    def test_pos_neg(self):
        self.assertEqual(s.negative_events, 1496)
        self.assertEqual(s.positive_events, 1856)
        self.assertEqual(s.events, 3352)

    def test_sharpe_ratio(self):
        self.assertAlmostEqual(s.sharpe_ratio(), 0.93655024411782517, places=10)

    def test_calmar_ratio(self):
        self.assertAlmostEqual(s.calmar_ratio(), 0.2168373273211153, places=10)

    def test_sortino_ratio(self):
        self.assertAlmostEqual(s.sortino_ratio(), 0.38088512888549675, places=10)

    def test_var(self):
        self.assertAlmostEqual(100*s.var(), 0.23009682589375524, places=10)

    def test_cvar(self):
        self.assertAlmostEqual(100*s.cvar(), 0.33727674011381015, places=10)
        #s = read_series("ts.csv", parse_dates=True)
        #x = 100*value_at_risk(s, alpha=0.99)
        #self.assertAlmostEqual(x, 0.40086450047240874, places=10)

    def test_autocorrelation(self):
        self.assertAlmostEqual(s.autocorrelation, 0.1235649973501317, places=10)

    def test_mtd(self):
        self.assertAlmostEqual(100*s.mtd, 1.4133604922211385, places=10)

    def test_ytd(self):
        self.assertAlmostEqual(100*s.ytd, 2.1718996734564122, places=10)

    def test_max_r(self):
        self.assertAlmostEqual(s.max_r, 0.0086040619154168496, places=10)

    def test_min_r(self):
        self.assertAlmostEqual(s.min_r, -0.012938315599174022, places=10)

    def test_max_nav(self):
        self.assertAlmostEqual(s.max_nav, 1.3358034259144032, places=10)
    #def test_cvar(self):
    #    s = read_series("ts.csv", parse_dates=True)
    #    x = 100*conditional_value_at_risk(s, alpha=0.99)
    #    self.assertAlmostEqual(x, 0.53542831745811131, places=10)

    def test_drawdown(self):
        self.assertAlmostEqual(100 * s.drawdown.max(), 5.7000575458488578, places=6)

    def test_monthly_table(self):
        self.assertAlmostEqual(100 * s.monthlytable["Nov"][2013], 0.23233078558395626, places=5)


    def test_ewm(self):
        self.assertAlmostEqual(100 * s.ewm_volatility(periods=250).values[-1], 2.7706672542422539, places=6)
        self.assertAlmostEqual(100 * s.ewm_ret(periods=250).values[-1], 6.0326401733122053, places=6)
        self.assertAlmostEqual(s.ewm_sharpe(periods=250).values[-1], 2.177323951144059, places=6)