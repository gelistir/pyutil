import pandas as pd
from pyutil.performance.adjust import adjust
from test.config import read_pd


def test_adjust():
    ts = read_pd("ts.csv", squeeze=True, header=None, parse_dates=True, index_col=0)
    assert adjust(ts).loc["2014-01-01"] == 100.00


def test_adjust_empty():
    assert adjust(pd.Series({})) is None
