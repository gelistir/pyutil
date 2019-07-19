import pandas as pd

import pandas.util.testing as pdt
import pytest

import pyutil.portfolio.analysis as ppa
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
        frame = ppa.drawdown(portfolios)
        pdt.assert_series_equal(frame["test"], portfolio.nav.drawdown, check_names=False)

    def test_mtd(self, portfolio, portfolios):
        frame = ppa.mtd(portfolios)
        assert frame.loc["test"]["Apr 20"] == portfolio.nav.mtd_series[pd.Timestamp("2015-04-20")]

    def test_ytd(self, portfolio, portfolios):
        frame = ppa.ytd(portfolios)
        assert frame.loc["test"]["03"] == portfolio.nav.ytd_series[pd.Timestamp("2015-03-31")]

    def test_recent(self, portfolio, portfolios):
        frame = ppa.recent(portfolios)
        assert frame.loc["test"]["Apr 20"] == portfolio.nav.recent(n=20)[pd.Timestamp("2015-04-20")]

    def test_performance(self, portfolios):
        frame = ppa.performance(portfolios)
        x = frame["test"]
        assert x["Kurtosis"] == "6.98"

    def test_ewm_volatility(self, portfolio, portfolios):
        frame = ppa.ewm_volatility(portfolios)
        pdt.assert_series_equal(frame["test"], portfolio.nav.ewm_volatility(), check_names=False)

    def test_period(self, portfolio, portfolios):
        frame = ppa.period(portfolios, before=pd.Timestamp("2015-01-01"))
        pdt.assert_series_equal(frame["test"], portfolio.nav.truncate(before=pd.Timestamp("2015-01-01"), adjust=True).series, check_names=False)
