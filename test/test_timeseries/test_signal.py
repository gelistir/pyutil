import pytest

from pyutil.timeseries.signal import trend_new
from test.config import read


@pytest.fixture()
def ts():
    return read("ts.csv", squeeze=True, header=None, parse_dates=True, index_col=0)


class TestSignal(object):
    def test_trend_new(self, ts):
        osc = trend_new(ts)
        assert osc["2015-04-20"] == pytest.approx(0.05390185343741904, 1e-6)

