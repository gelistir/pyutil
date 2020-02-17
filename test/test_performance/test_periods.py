import pandas as pd
import pytest

from pyutil.performance.periods import periods, period_returns
from test.config import read


@pytest.fixture(scope="module")
def returns():
    return read("ts.csv", squeeze=True, header=None, parse_dates=True, index_col=0).pct_change()


def test_periods():
    p = periods(today=pd.Timestamp("2015-05-01"))
    assert p["Two weeks"].start == pd.Timestamp("2015-04-17")
    assert p["Two weeks"].end == pd.Timestamp("2015-05-01")


def test_period_returns(returns):
    p = periods(today=pd.Timestamp("2015-05-01"))
    x = 100*period_returns(returns=returns, offset=p)
    assert x["Three Years"] == pytest.approx(1.1645579858904798, 1e-10)


# def test_periods_more(prices):
#     y = period_prices(prices, offset=periods(today=prices.index[-1]))
#     pdt.assert_series_equal(y, read("periods.csv", header=None, squeeze=True, index_col=0), check_names=False)
#
#
# def test_period_returns_without_periods(prices):
#     x = 100*period_prices(prices, today=pd.Timestamp("2015-05-01"))
#     assert x["Three Years"] == pytest.approx(1.1645579858904798 , 1e-10)
