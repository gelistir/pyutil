import pandas as pd
import pandas.util.testing as pdt
import pytest

from pyutil.performance.periods import periods, period_returns
from test.config import read


@pytest.fixture(scope="module")
def returns():
    return read("ts.csv", squeeze=True, index_col=0, header=None, parse_dates=True).pct_change().dropna()



class TestPeriods(object):
    def test_periods(self):
        p = periods(today=pd.Timestamp("2015-05-01"))
        assert p["Two weeks"].start == pd.Timestamp("2015-04-17")
        assert p["Two weeks"].end == pd.Timestamp("2015-05-01")

    def test_period_returns(self, returns):
        p = periods(today=pd.Timestamp("2015-05-01"))
        x = 100*period_returns(returns=returns, offset=p)
        assert x["Three Years"] == pytest.approx(1.1645579858904798, 1e-10)

    def test_periods_more(self, returns):
        y = period_returns(returns, offset=periods(today=returns.index[-1]))
        pdt.assert_series_equal(y, read("periods.csv", parse_dates=False, header=None, index_col=0, squeeze=True), check_names=False)

    def test_period_returns_without_periods(self, returns):
        x = 100*period_returns(returns=returns, today=pd.Timestamp("2015-05-01"))
        assert x["Three Years"] == pytest.approx(1.1645579858904798 , 1e-10)
