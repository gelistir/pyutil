import pandas as pd
import pandas.util.testing as pdt
import pytest

from pyutil.performance.drawdown import Drawdown, drawdown
from test.config import read_pd


@pytest.fixture(scope="module")
def ts():
    return read_pd("ts.csv", squeeze=True, header=None, parse_dates=True, index_col=0).pct_change().fillna(0.0)


@pytest.fixture()
def dd():
    return read_pd("drawdown.csv", squeeze=True, parse_dates=True, header=None, index_col=0)


def test_drawdown(ts, dd):
    pdt.assert_series_equal(Drawdown(ts).drawdown, dd, check_names=False)
    pdt.assert_series_equal(drawdown(ts), dd, check_names=False)


def test_empty():
    x = pd.Series({})
    pdt.assert_series_equal(Drawdown(x).drawdown, pd.Series({}))


def test_negative_price():
    x = pd.Series({0: -1.1, 1: 1, 2: 2})
    with pytest.raises(AssertionError):
        Drawdown(x)


def test_wrong_index_order():
    x = pd.Series(index=[0, 2, 1], data=[1, 1, 1])
    with pytest.raises(AssertionError):
        Drawdown(x)


def test_eps():
    x = pd.Series({})
    assert Drawdown(x, eps=1e-10).eps == 1e-10
