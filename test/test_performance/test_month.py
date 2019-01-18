import pandas.util.testing as pdt
import pytest

from pyutil.performance._month import _monthlytable
from test.config import read


@pytest.fixture(scope="module")
def ts():
    return read("ts.csv", squeeze=True, index_col=0, header=None, parse_dates=True)

class TestMonth(object):
    def test_table(self, ts):
        pdt.assert_almost_equal(_monthlytable(ts), read("monthtable.csv", parse_dates=False, index_col=0, header=0))
