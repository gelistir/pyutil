import pytest
import pandas.util.testing as pdt
from pyutil.performance.month import monthlytable
from test.config import read


@pytest.fixture(scope="module")
def ts():
    return read("ts.csv", squeeze=True, header=None, parse_dates=True, index_col=0)


class TestMonth(object):
    def test_table(self, ts):
        pdt.assert_almost_equal(monthlytable(ts), read("monthtable.csv", index_col=0))
