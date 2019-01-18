import pandas as pd
import pytest

from pyutil.timeseries.signal import trend
from test.config import read

s = read("ts.csv", squeeze=True, index_col=0, header=None, parse_dates=True)

index = pd.Timestamp("2015-04-14")


class TestSignal(object):
    def test_trend(self):
        assert trend(s)[index] == pytest.approx(-0.06181926927450359, 1e-5)
