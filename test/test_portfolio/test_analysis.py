import pandas as pd

import pandas.util.testing as pdt
import pytest

import pyutil.portfolio.analysis as ppa
from pyutil.portfolio.format import percentage
from test.config import test_portfolio


@pytest.fixture(scope="module")
def portfolio():
    return test_portfolio()


@pytest.fixture(scope="module")
def portfolios(portfolio):
    return {"test": portfolio}


class TestAnalysis(object):
    def test_nav(self, portfolio, portfolios):
        frame = ppa.nav(portfolios)
        pdt.assert_series_equal(frame["test"], portfolio.nav.series, check_names=False)

    def test_drawdown(self, portfolio, portfolios):
        frame = ppa.drawdown(ppa.nav(portfolios))
        pdt.assert_series_equal(frame["test"], portfolio.nav.drawdown, check_names=False)

    def test_mtd(self, portfolio, portfolios):
        xxx = ppa.mtd(frame=ppa.nav(portfolios))
        assert xxx.loc["test"]["Apr 20"] == portfolio.nav.mtd_series[pd.Timestamp("2015-04-20")]

    def test_ytd(self, portfolio, portfolios):
        xxx = ppa.ytd(frame=ppa.nav(portfolios))
        assert xxx.loc["test"]["03"] == portfolio.nav.ytd_series[pd.Timestamp("2015-03-31")]

    def test_recent(self, portfolio, portfolios):
        xxx = ppa.recent(ppa.nav(portfolios))
        assert xxx.loc["test"]["Apr 20"] == portfolio.nav.recent(n=20)[pd.Timestamp("2015-04-20")]

    def test_performance(self, portfolios):
        frame = ppa.performance(ppa.nav(portfolios))
        assert frame["test"]["Kurtosis"] == "6.98"

    def test_ewm_volatility(self, portfolio, portfolios):
        frame = ppa.ewm_volatility(ppa.nav(portfolios))
        pdt.assert_series_equal(frame["test"], portfolio.nav.ewm_volatility(), check_names=False)

    def test_sector(self, portfolio, portfolios):
        symbolmap = {"A": "A", "B": "A", "C": "B", "D": "B", "E": "C", "F": "C", "G": "C"}

        frame = ppa.sector(portfolios=portfolios, symbolmap=symbolmap).applymap(percentage)
        assert frame["test"]["A"] == "13.57%"
        pdt.assert_series_equal(frame["test"], portfolio.sector(symbolmap=symbolmap).iloc[-1].apply(percentage), check_names=False)
