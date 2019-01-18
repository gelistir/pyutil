import pandas as pd
import pytest

from pyutil.performance._drawdown import _Drawdown
from pyutil.test.aux import read_series
from test.config import resource, read

ts = read_series(resource("ts.csv"), parse_dates=True)

import pandas.util.testing as pdt

@pytest.fixture(scope="module")
def ts():
    return read("ts.csv", squeeze=True, index_col=0, header=None, parse_dates=True)


class TestDrawdown(object):
    def test_drawdown(self, ts):
        pdt.assert_series_equal(_Drawdown(ts).drawdown, read("drawdown.csv", squeeze=True, index_col=0, parse_dates=True, header=None), check_names=False)

    def test_periods(self, ts):
        x = _Drawdown(ts).periods
        assert x[pd.Timestamp("2014-03-07")] == pd.Timedelta(days=66)

    def test_empty(self):
        x = pd.Series({})
        pdt.assert_series_equal(_Drawdown(x).drawdown, pd.Series({}))

    def test_negative_price(self):
        x = pd.Series({0: 3, 1: 2, 2: -2})
        with pytest.raises(AssertionError):
            _Drawdown(x)

    def test_wrong_index_order(self):
        x = pd.Series(index=[0, 2, 1], data=[1, 1, 1])
        with pytest.raises(AssertionError):
            _Drawdown(x)

    def test_eps(self):
        x = pd.Series({})
        assert _Drawdown(x, eps=1e-10).eps == 1e-10

    def test_series(self):
        x = pd.Series({0: 3, 1: 2, 2: 1})
        pdt.assert_series_equal(x, _Drawdown(x).price_series)