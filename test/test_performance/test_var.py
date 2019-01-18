import pytest

from pyutil.performance._var import _VaR
from test.config import read

class TestVar(object):
    def test_class(self):
        ts = read("ts.csv", parse_dates=True, index_col=0, header=None, squeeze=True)

        v = _VaR(ts, alpha=0.99)
        assert 100*v.cvar == pytest.approx(0.51218385609772821, 1e-10)
        assert 100*v.var == pytest.approx(0.47550914363392316, 1e-10)