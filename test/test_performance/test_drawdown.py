import pandas as pd
from pyutil.performance._drawdown import _Drawdown
from test.config import read_series
from unittest import TestCase

ts = read_series("ts.csv", parse_dates=True)

import pandas.util.testing as pdt


class TestDrawdown(TestCase):
    def test_drawdown(self):
        pdt.assert_series_equal(_Drawdown(ts).drawdown, read_series("drawdown.csv", parse_dates=True), check_names=False)

    def test_periods(self):
        x = _Drawdown(ts).periods
        self.assertEqual(x[pd.Timestamp("2014-03-07")], pd.Timedelta(days=66))

    def test_empty(self):
        x = pd.Series({})
        pdt.assert_series_equal(_Drawdown(x).drawdown, pd.Series({}))

    def test_negative_price(self):
        x = pd.Series({0: 3, 1: 2, 2: -2})
        with self.assertRaises(AssertionError):
            _Drawdown(x)

    def test_wrong_index_order(self):
        x = pd.Series(index=[0, 2, 1], data=[1, 1, 1])
        with self.assertRaises(AssertionError):
            _Drawdown(x)

    def test_eps(self):
        x = pd.Series({})
        self.assertEqual(_Drawdown(x, eps=1e-10).eps, 1e-10)