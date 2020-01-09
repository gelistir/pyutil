import pytest

from pyutil.timeseries.signal import trend_new, volatility
from test.config import read


@pytest.fixture()
def ts():
    return read("ts.csv", squeeze=True, header=None, parse_dates=True, index_col=0)


def test_trend_new(ts):
    osc = trend_new(ts)
    assert osc["2015-04-20"] == pytest.approx(0.05457355730789621, 1e-6)


def test_volatility(ts):
    vola = volatility(prices=ts, annualized=True)
    assert vola["2015-04-20"] == pytest.approx(0.02850826439271097, 1e-6)
