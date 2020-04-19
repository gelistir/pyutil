import pandas as pd
import pytest

from pyutil.performance.periods import periods, period_returns
from test.config import read_pd


def test_periods():
    p = periods(today=pd.Timestamp("2015-05-01"))
    assert p["Two weeks"].start == pd.Timestamp("2015-04-17")
    assert p["Two weeks"].end == pd.Timestamp("2015-05-01")


def test_period_returns():
    returns = read_pd("ts.csv", squeeze=True, header=None, parse_dates=True, index_col=0).pct_change()
    p = periods(today=pd.Timestamp("2015-05-01"))
    x = 100*period_returns(returns=returns, offset=p)
    assert x["Three Years"] == pytest.approx(1.1645579858904798, 1e-10)

