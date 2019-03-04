import pandas as pd
import pytest

from pyutil.cvx.cvx import elasticNet
from test.config import read

class TestCvx(object):
    def test_elasticNet(self):
        prices = read("smi2017_adjclose.txt", index_col="Date", parse_dates=True).sort_index()
        matrix = prices.pct_change().fillna(0.0)
        w0 =  pd.Series(index=matrix.columns, data=0.05)

        w = elasticNet(matrix, w0.values, lamb_balance=0.4, lamb_trades=0.03)
        assert w["SGSN.VX"] == pytest.approx(0.052298, abs=1e-6)
