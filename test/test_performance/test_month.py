from unittest import TestCase

from pyutil.performance._month import _monthlytable

import pandas.util.testing as pdt

from pyutil.test.aux import read_series, read_frame
from test.config import resource


class TestMonth(TestCase):
    def test_table(self):
        s = read_series(resource("ts.csv"))
        pdt.assert_almost_equal(_monthlytable(s), read_frame(resource("monthtable.csv"), parse_dates=False))
