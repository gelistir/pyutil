import pandas.util.testing as pdt
from pyutil.performance.month import monthlytable
from test.config import read_pd


def test_table():
    ts = read_pd("ts.csv", squeeze=True, header=None, parse_dates=True, index_col=0).pct_change().fillna(0.0)
    pdt.assert_almost_equal(monthlytable(ts), read_pd("monthtable.csv", index_col=0))
