import pytest

from pyutil.performance.var import VaR, var, cvar
from test.config import read_pd


@pytest.fixture(scope="module")
def ts():
    return read_pd("ts.csv", squeeze=True, header=None, parse_dates=True, index_col=0).pct_change().fillna(0.0)


def test_class(ts):
    v = VaR(ts, alpha=0.99)
    assert 100*v.cvar == pytest.approx(0.51218385609772821, 1e-10)
    assert 100*v.var == pytest.approx(0.47550914363392316, 1e-10)
    assert 100*var(ts) == pytest.approx(0.47550914363392316, 1e-10)
    assert 100*cvar(ts) == pytest.approx(0.51218385609772821, 1e-10)