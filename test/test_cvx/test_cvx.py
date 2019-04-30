import pandas as pd
import pytest

from pyutil.cvx.cvx import *
from test.config import read


class TestCvx(object):
    def test_elasticNet(self):
        prices = read("smi2017_adjclose.txt", index_col="Date", parse_dates=True).sort_index()
        matrix = prices.pct_change().fillna(0.0)
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
