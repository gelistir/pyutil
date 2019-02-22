import pandas.util.testing as pdt
from pyutil.performance.month import monthlytable
from test.config import read


class TestMonth(object):
    def test_table(self):
        ts = read("ts.csv", squeeze=True, header=None, parse_dates=True)
        pdt.assert_almost_equal(monthlytable(ts), read("monthtable.csv"))
