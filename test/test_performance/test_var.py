import pytest

from pyutil.performance.var import VaR
from test.config import read

@pytest.fixture(scope="module")
def ts():
    return read("ts.csv", squeeze=True, header=None, parse_dates=True)

class TestVar(object):
    def test_class(self, ts):
        v = VaR(ts, alpha=0.99)
        assert 100*v.cvar == pytest.approx(0.51218385609772821, 1e-10)
        assert 100*v.var == pytest.approx(0.47550914363392316, 1e-10)