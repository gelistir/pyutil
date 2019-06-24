import pytest

from pyutil.cvx.cvx import *
from test.config import read
import pandas.util.testing as pdt


@pytest.fixture(scope="module")
def matrix():
    return read("smi2017_adjclose.txt", index_col="Date", parse_dates=True).sort_index().pct_change().fillna(0.0)


class TestCvx(object):
    def test_elasticNet(self, matrix):
        w0 = pd.Series(index=matrix.columns, data=0.05)
        w = elasticNet(matrix, w0.values, lamb_balance=0.4, lamb_trades=0.03)
        assert w["SGSN.VX"] == pytest.approx(0.052298, abs=1e-6)

    #def test_convexProgram(self):
    #    prices = read("smi2017_adjclose.txt", index_col="Date", parse_dates=True).sort_index()
    #    x = cvx.Variable(prices.shape[1])
    #
    #    maximize(objective=objective, constraints=[0 <= x, cvx.sum(x) == 1])

    def test_mean_variation(self):
        x = pd.Series(data=[5.0, 4.0, 1.0, 5.0, 0.0, -2.0, -3.0, 3.0])
        assert mean_variation(ts=x) == pytest.approx(3.142857142857143, 1e-10)

    def test_minimize(self, matrix):
        w = cvx.Variable(matrix.shape[1])
        objective = cvx.norm(matrix.values * w, 2)
        minimize(objective=objective, constraints=[0 <= w, cvx.sum(w) == 1])
        x = pd.Series(index=matrix.keys(), data=w.value)
        assert x["UBSG.VX"] == pytest.approx(0.003883682829749637, 1e-6)

    # def test_std(self, matrix):
    #     w = cvx.Variable(matrix.shape[1])
    #     objective = cvx.norm(matrix.values * w, 2)
    #     minimize(objective=objective, constraints=[0 <= w, std(w)==1])
    #     x = pd.Series(index=matrix.keys(), data=w.value)
    #     print(x)
    #     pdt.assert_series_equal(x, pd.Series(index=["A"], data=[0.1]))

    def test_maximize(self, matrix):
        w = cvx.Variable(matrix.shape[1])
        objective = cvx.norm(matrix.values * w, 2)
        maximize(objective=-objective, constraints=[0 <= w, cvx.sum(w) == 1])
        x = pd.Series(index=matrix.keys(), data=w.value)
        assert x["UBSG.VX"] == pytest.approx(0.003883682829749637, 1e-6) # 1.0, 1e-6)

    def test_signals(self, matrix):
        x = signals(matrix, 0.1*np.ones(matrix.shape[0]))
        assert x["UBSG.VX"] == pytest.approx(-0.008692211362626311, 1e-6)

