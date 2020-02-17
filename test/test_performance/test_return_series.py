import pandas as pd
import pytest

import pandas.util.testing as pdt

from pyutil.performance.return_series import from_nav
from test.config import read


@pytest.fixture()
def ts():
    ts = read("ts.csv", parse_dates=True, squeeze=True, header=None, index_col=0)
    ts.name = None
    ts.index.name = None
    return from_nav(ts)


def test_from_nav():
    t1 = pd.Timestamp("2010-10-21")
    t2 = pd.Timestamp("2010-10-22")
    t3 = pd.Timestamp("2010-10-23")
    nav = pd.Series(index=[t1, t2, t3], data=[10.0, 10.5, 9.9])
    x = from_nav(nav)
    pdt.assert_series_equal(10*x.nav, nav)




def test_sharpe(ts):
    assert ts.sharpe_ratio() == pytest.approx(0.3243176318150913, 1e-10)


def test_events(ts):
    assert ts.size == 341
    assert ts.positive_events == 177
    assert ts.negative_events == 164


def test_var(ts):
    assert pytest.approx(ts.var(), 1e-10) == 0.002934149490912419
    assert pytest.approx(ts.cvar(), 1e-10) == 0.003953842677691919


def test_monthlytable(ts):
    assert 100 * ts.monthlytable["Nov"][2014] == pytest.approx(-0.19540358586001005, 1e-10)
    assert 100 * ts.monthlytable["YTD"][2015] == pytest.approx(2.171899673456368, 1e-10)


def test_periodreturns(ts):
    assert 100*ts.period_returns["Year-to-Date"] == pytest.approx(2.171899673456368, 1e-10)


def test_annual_returns(ts):
    assert 100*ts.annual_returns.loc[2015] == pytest.approx(2.171899673456368, 1e-10)
    x = ts.monthly_returns.loc[2015,:]
    assert 100*((x + 1).prod()-1.0) == pytest.approx(2.171899673456368, 1e-10)


def test_drawdown(ts):
    assert 100*ts.drawdown.max() == pytest.approx(3.988575670566674, 1e-10)


def test_ewm_volatility(ts):
    x = ts.ewm_volatility(periods=256)
    pdt.assert_series_equal(x, 16 * ts.ewm(com=50, min_periods=50).std(bias=False))


def test_mtd(ts):
    assert ts.tail_month.index[0] == pd.Timestamp("2015-04-01")
    assert ts.tail_year.index[0] == pd.Timestamp("2015-01-01")


def test_sortino(ts):
    assert ts.sortino_ratio() == pytest.approx(0.22218777678862034, 1e-10)


def test_calmar(ts):
    assert ts.calmar_ratio() == pytest.approx(0.22218777678862034, 1e-10)

def test_tail_month(ts):
    assert ts.tail_month.index[0] == pd.Timestamp("2015-04-01")
    pdt.assert_series_equal(ts.tail_month.resample("M"), pd.Series(index=[pd.Timestamp("2015-04-30")], data=0.014133604922211163), check_exact=False)

def test_tail_year(ts):
    assert ts.tail_year.index[0] == pd.Timestamp("2015-01-01")
    assert ts.tail_year.resample("M").size == 4

#@pytest.fixture()
#def nav(ts):
#    return fromNav(ts, adjust=True)


#def test_annual(ts):
#    print(ts.mtd(today=ts.index[-1]))
#    print(ts.ytd(today=ts.index[-1]).monthly_returns)
#    print(ts.ewm_volatility(com=20))
#    assert False
