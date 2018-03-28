from unittest import TestCase

import pandas as pd

from pyutil.performance.drawdown import drawdown
from pyutil.performance.summary import fromNav
from pyutil.web.aux import reset_index
from pyutil.web.post import post_month, post_perf, post_drawdown, post_volatility
from test.config import read_series, series2arrays

import pandas.util.testing as pdt

class TestPost(TestCase):
    @classmethod
    def setUpClass(cls):
        cls.nav = read_series("nav.csv", parse_dates=True)
        cls.nav.index = [a.date() for a in cls.nav.index]

        cls.data = series2arrays(cls.nav)

    def test_post_month(self):
        x = post_month(data=self.data)
        self.assertAlmostEqual(x["Feb"]["2015"], 0.005928, places=5)

    def test_post_perf(self):
        x = post_perf(data=self.data)
        self.assertEqual(x.loc["First_at"], "2014-12-11")
        self.assertEqual(x.loc["YTD"], "1.53")

    def test_post_drawdown(self):
        x = post_drawdown(data=self.data)
        x.index = [a.date() for a in x.index]
        pdt.assert_series_equal(drawdown(self.nav), x)

    def test_post_volatility(self):
        x = post_volatility(data=self.data)
        x.index = [a.date() for a in x.index]
        pdt.assert_series_equal(fromNav(self.nav).ewm_volatility(), x)

    def test_reset_index(self):
        x = pd.DataFrame(index=["A"], columns=["Wurst"], data=[[2.0]])
        b = reset_index(x)
        self.assertListEqual(b["columns"], ["Name", "Wurst"])
