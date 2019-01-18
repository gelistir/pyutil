from unittest import TestCase

import pandas as pd
import pandas.util.testing as pdt

from pyutil.testing.aux import read_frame
from test.config import test_portfolio, resource

from pyutil.sql.interfaces.symbols.portfolio import Portfolio
from pyutil.sql.interfaces.symbols.symbol import Symbol, SymbolType

t1 = pd.Timestamp("2010-04-24")
t2 = pd.Timestamp("2010-04-25")


import pytest

@pytest.fixture
def portfolio():
    p = Portfolio(name="Maffay")

    x = dict()
    for name in ["A", "B", "C"]:
        x[name] = Symbol(name=name, group=SymbolType.equities)

    for name in ["D", "E", "F", "G"]:
        x[name] = Symbol(name=name, group=SymbolType.fixed_income)

    p.upsert(portfolio=test_portfolio(), symbols=x)

    return p

class TestPortfolio(object):
    def test_read(self, portfolio):
        p = test_portfolio()

        pdt.assert_frame_equal(portfolio.weights, p.weights, check_names=False)
        pdt.assert_frame_equal(portfolio.prices, p.prices, check_names=False)

        pdt.assert_series_equal(portfolio.nav, p.nav.series, check_names=False)
        pdt.assert_series_equal(portfolio.leverage, p.leverage, check_names=False)

    def test_upsert(self, portfolio):
        p = 5*portfolio.portfolio.tail(10)
        pp = portfolio.upsert(p, symbols=None)
        print(type(pp))
        assert isinstance(pp, Portfolio)


        x = portfolio.weights.tail(12).sum(axis=1)
        assert x["2015-04-08"] == pytest.approx(0.305048, 1e-5)
        assert x["2015-04-09"] == pytest.approx(1.524054, 1e-5)

    def test_last(self, portfolio):
        assert portfolio.prices.last_valid_index() == pd.Timestamp("2015-04-22")

    def test_sector(self, portfolio):
        pdt.assert_series_equal(portfolio.sector(total=False).loc["2015-04-22"], pd.Series(index=["Equities","Fixed Income"], data=[0.135671, 0.173303]), check_names=False)
        pdt.assert_series_equal(portfolio.sector(total=True).loc["2015-04-22"], pd.Series(index=["Equities", "Fixed Income", "Total"], data=[0.135671, 0.173303, 0.3089738755]), check_names=False)

    def test_state(self, portfolio):
        pdt.assert_frame_equal(read_frame(resource("state.csv")), portfolio.state, check_names=False, check_exact=False, check_less_precise=True, check_dtype=False)

