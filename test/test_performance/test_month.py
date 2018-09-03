from unittest import TestCase
from test.config import read_series, read_frame

from pyutil.performance._month import _monthlytable
import pandas.util.testing as pdt


class TestMonth(TestCase):
    def test_table(self):
        s = read_series("ts.csv", parse_dates=True)
        pdt.assert_almost_equal(_monthlytable(s), read_frame("monthtable.csv", parse_dates=False))
