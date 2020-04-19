import pandas as pd

import pandas.util.testing as pdt
import pytest

import pyutil.portfolio.analysis as ppa
from pyutil.performance.return_series import drawdown, from_nav
from pyutil.portfolio.format import percentage
from test.config import portfolio


@pytest.fixture()
def portfolios(portfolio):
    return {"test": portfolio, "A": None}


@pytest.fixture()
def navs(portfolio):
    return {"test": portfolio.nav, "A": None}


@pytest.fixture()
def returns(portfolio):
    return {"test": portfolio.returns, "A": None}


def test_nav(portfolio, portfolios):
    frame = ppa.nav(portfolios)
    pdt.assert_series_equal(frame["test"], portfolio.nav, check_names=False)


def test_returns(portfolio, portfolios):
    frame = ppa.returns(portfolios)
    pdt.assert_series_equal(frame["test"], portfolio.returns, check_names=False)


def test_drawdown(portfolio, navs):
    frame = ppa.drawdown(navs)
    pdt.assert_series_equal(frame["test"], drawdown(portfolio.returns), check_names=False)
    assert set(frame.keys()) == {"test"}


def test_mtd(portfolio, navs):
    xxx = ppa.mtd(frame=navs)
    assert xxx.loc["test"]["Apr 20"] == from_nav(portfolio.nav).tail_month[pd.Timestamp("2015-04-20")]
    assert set(xxx.index) == {"test"}


def test_ytd(portfolio, navs):
    xxx = ppa.ytd(frame=navs)
    assert xxx.loc["test"]["03"] == from_nav(portfolio.nav).tail_year.resample("M")[pd.Timestamp("2015-03-31")]
    assert set(xxx.index) == {"test"}


def test_recent(portfolio, navs):
    x = ppa.recent(navs)
    assert x.loc["test"]["Apr 20"] == from_nav(portfolio.nav).recent(n=20)[pd.Timestamp("2015-04-20")]
    assert set(x.index) == {"test"}


def test_performance(navs):
    frame = ppa.performance(navs)
    assert frame["test"]["Kurtosis"] == "7.00"
    assert set(frame.keys()) == {"test"}


def test_ewm_volatility(portfolio, navs):
    frame = ppa.ewm_volatility(navs)
    pdt.assert_series_equal(frame["test"], from_nav(portfolio.nav).ewm_volatility(), check_names=False)


def test_sector(portfolio, portfolios):
    symbolmap = {"A": "A", "B": "A", "C": "B", "D": "B", "E": "C", "F": "C", "G": "C"}

    frame = ppa.sector(portfolios=portfolios, symbolmap=symbolmap).applymap(percentage)
    assert frame["test"]["A"] == "13.57%"
    pdt.assert_series_equal(frame["test"], portfolio.sector(symbolmap=symbolmap).iloc[-1].apply(percentage), check_names=False)
    assert set(frame.keys()) == {"test"}
