import pandas as pd
import pandas.util.testing as pdt
import pytest

from pyutil.timeseries.merge import merge, last_index, first_index, to_datetime, to_date
from test.config import read


@pytest.fixture()
def ts():
    return read("ts.csv", squeeze=True, header=None, parse_dates=True, index_col=0)

#index = pd.Timestamp("2015-04-14")


class TestMerge(object):
    def test_last_index(self, ts):
        t = last_index(ts)
        t0 = pd.Timestamp("2015-04-22")
        assert t == t0
        assert not last_index(None)
        assert last_index(None, default=t0) == t0
        assert last_index(pd.Series({}), default=t0) == t0

    def test_first_index(self, ts):
        t = first_index(ts)
        t0 = pd.Timestamp("2014-01-01")
        assert t == t0
        assert not first_index(None)
        assert first_index(None, default=t0) == t0
        assert first_index(pd.Series({}), default=t0) == t0

    def test_merge(self, ts):
        x = merge(new=ts)
        pdt.assert_series_equal(x, ts)

        x = merge(new=ts, old=ts)
        pdt.assert_series_equal(x, ts)

        x = merge(new=5*ts, old=ts)
        pdt.assert_series_equal(x, 5*ts)

        y = merge(None)
        assert not y

        y = merge(pd.Series({}), None)
        pdt.assert_series_equal(y, pd.Series({}))

        y = merge(pd.Series({}), pd.Series({}))
        pdt.assert_series_equal(y, pd.Series({}))

    def test_to_datetime(self):
        assert not to_datetime(None)

        t0 = pd.Timestamp("2015-04-22")
        x = pd.Series(index=[t0], data=[2.0])

        # should be safe to apply to_datetime
        pdt.assert_series_equal(x, to_datetime(x))

    def test_to_date(self):
        assert not to_date(None)

        t0 = pd.Timestamp("2015-04-22")
        x = pd.Series(index=[t0], data=[2.0])

        pdt.assert_series_equal(pd.Series(index=[t0.date()], data=[2.0]), to_date(x))
        pdt.assert_series_equal(to_date(ts=x, format="%Y%m%d"), pd.Series(index=["20150422"], data=[2.0]))
