import pandas as pd
import pandas.util.testing as pdt
import pytest

from pyutil.performance.periods import periods, period_prices
from test.config import read


@pytest.fixture(scope="module")
def prices():
    return read("ts.csv", squeeze=True, header=None, parse_dates=True)


class TestPeriods(object):
    def test_periods(self):
        p = periods(today=pd.Timestamp("2015-05-01"))
        assert p["Two weeks"].start == pd.Timestamp("2015-04-17")
        assert p["Two weeks"].end == pd.Timestamp("2015-05-01")

    def test_period_returns(self, prices):
        p = periods(today=pd.Timestamp("2015-05-01"))
        x = 100*period_prices(prices=prices, offset=p)
        assert x["Three Years"] == pytest.approx(1.1645579858904798, 1e-10)

    def test_periods_more(self, prices):
        y = period_prices(prices, offset=periods(today=prices.index[-1]))
        pdt.assert_series_equal(y, read("periods.csv", header=None, squeeze=True), check_names=False)

    def test_period_returns_without_periods(self, prices):
        x = 100*period_prices(prices, today=pd.Timestamp("2015-05-01"))
        assert x["Three Years"] == pytest.approx(1.1645579858904798 , 1e-10)
