import pandas as pd
import pytest

from pyutil.performance.adjust import adjust
from test.config import read


@pytest.fixture(scope="module")
def ts():
    return read("ts.csv", squeeze=True, header=None, parse_dates=True, index_col=0)


class TestAdjust(object):
    def test_adjust(self, ts):
        assert adjust(ts).loc["2014-01-01"] == 100.00

    def test_adjust_empty(self):
        assert adjust(pd.Series({})).empty
