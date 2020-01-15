import pandas as pd

import pandas.util.testing as pdt
import pytest

import pyutil.portfolio.analysis as ppa
from pyutil.portfolio.format import percentage
from test.config import portfolio


@pytest.fixture()
def portfolios(portfolio):
    return {"test": portfolio, "A": None}


@pytest.fixture()
def navs(portfolio):
    return {"test": portfolio.nav, "A": None}


class TestAnalysis(object):
    def test_nav(self, portfolio, portfolios):
        frame = ppa.nav(portfolios)
        pdt.assert_series_equal(frame["test"], portfolio.nav.series, check_names=False)

    def test_drawdown(self, portfolio, navs):
        frame = ppa.drawdown(navs)
        pdt.assert_series_equal(frame["test"], portfolio.nav.drawdown, check_names=False)
        assert set(frame.keys()) == {"test"}

    def test_mtd(self, portfolio, navs):
        xxx = ppa.mtd(navs)
        assert xxx.loc["test"]["Apr 20"] == portfolio.nav.mtd_series[pd.Timestamp("2015-04-20")]
        assert set(xxx.index) == {"test"}

    def test_ytd(self, portfolio, navs):
        xxx = ppa.ytd(frame=navs)
        assert xxx.loc["test"]["03"] == portfolio.nav.ytd_series[pd.Timestamp("2015-03-31")]
        assert set(xxx.index) == {"test"}

    def test_recent(self, portfolio, navs):
        xxx = ppa.recent(navs)
        assert xxx.loc["test"]["Apr 20"] == portfolio.nav.recent(n=20)[pd.Timestamp("2015-04-20")]
        assert set(xxx.index) == {"test"}

        x = pd.DataFrame(navs)
        xxx2 = ppa.recent(x)
        assert xxx2.loc["test"]["Apr 20"] == portfolio.nav.recent(n=20)[pd.Timestamp("2015-04-20")]
        assert set(xxx2.index) == {"test"}

    def test_performance(self, navs):
        frame = ppa.performance(navs)
        assert frame["test"]["Kurtosis"] == "6.98"
        assert set(frame.keys()) == {"test"}

    def test_ewm_volatility(self, portfolio, navs):
        frame = ppa.ewm_volatility(navs)
        pdt.assert_series_equal(frame["test"], portfolio.nav.ewm_volatility(), check_names=False)

    def test_sector(self, portfolio, portfolios):
        symbolmap = {"A": "A", "B": "A", "C": "B", "D": "B", "E": "C", "F": "C", "G": "C"}

        frame = ppa.sector(portfolios=portfolios, symbolmap=symbolmap).applymap(percentage)
        assert frame["test"]["A"] == "13.57%"
        pdt.assert_series_equal(frame["test"], portfolio.sector(symbolmap=symbolmap).iloc[-1].apply(percentage), check_names=False)
        assert set(frame.keys()) == {"test"}
