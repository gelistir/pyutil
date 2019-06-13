import pandas as pd
import pandas.util.testing as pdt
import pytest

from pyutil.performance.drawdown import Drawdown
from test.config import read


@pytest.fixture(scope="module")
def ts():
    return read("ts.csv", squeeze=True, header=None, parse_dates=True, index_col=0)


class TestDrawdown(object):
    def test_drawdown(self, ts):
        pdt.assert_series_equal(Drawdown(ts).drawdown, read("drawdown.csv", squeeze=True, parse_dates=True, header=None, index_col=0), check_names=False)

    def test_periods(self, ts):
        x = Drawdown(ts).periods
        assert x[pd.Timestamp("2014-03-07")] == pd.Timedelta(days=66)

    def test_empty(self):
        x = pd.Series({})
        pdt.assert_series_equal(Drawdown(x).drawdown, pd.Series({}))

    def test_negative_price(self):
        x = pd.Series({0: 3, 1: 2, 2: -2})
        with pytest.raises(AssertionError):
            Drawdown(x)

    def test_wrong_index_order(self):
        x = pd.Series(index=[0, 2, 1], data=[1, 1, 1])
        with pytest.raises(AssertionError):
            Drawdown(x)

    def test_eps(self):
        x = pd.Series({})
        assert Drawdown(x, eps=1e-10).eps == 1e-10

    def test_series(self):
        x = pd.Series({0: 3, 1: 2, 2: 1})
        pdt.assert_series_equal(x, Drawdown(x).price_series)