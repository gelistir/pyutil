import pandas as pd
import numpy as np
import pytest

from pyutil.performance.summary import performance, fromNav, fromReturns

import pandas.util.testing as pdt

from test.config import read


@pytest.fixture()
def ts():
    ts = read("ts.csv", parse_dates=True, squeeze=True, header=None, index_col=0)
    ts.name = None
    ts.index.name = None
    return ts


@pytest.fixture()
def nav(ts):
    return fromNav(ts, adjust=True)


@pytest.fixture()
def summary():
    return read("summary.csv", header=None, index_col=0, squeeze=True).apply(str)


class TestSummary(object):
    def test_adjust(self, nav):
        x = nav.adjust(value=10000)
        assert x.loc[0] == 10000.0

        y = 10000 * fromNav(nav, adjust=True)
        pdt.assert_series_equal(x, y)

    def test_summary(self, nav, summary):
        pdt.assert_series_equal(nav.summary().apply(str), summary, check_names=False, check_exact=False)

    def test_mtd(self, nav):
        assert 100 * nav.mtd == pytest.approx(1.4133604922211385, 1e-10)

        x = pd.Series(index=[pd.Timestamp("2017-01-04"), pd.Timestamp("2017-01-06")], data=[1.0, 1.6])
        assert fromNav(x).mtd == pytest.approx(0.6, 1e-10)

    def test_ytd(self, nav):
        assert 100 * nav.ytd == pytest.approx(2.1718996734564122, 1e-10)

        x = pd.Series(index=[pd.Timestamp("2017-01-04"), pd.Timestamp("2017-03-06")], data=[1.0, 1.6])
        assert fromNav(x).mtd == pytest.approx(0.6, 1e-10)
        assert fromNav(x).ytd == pytest.approx(0.6, 1e-10)

    def test_monthly_table(self, nav):
        assert 100 * nav.monthlytable["Nov"][2014] == pytest.approx(-0.19540358586001005, 1e-10)

    def test_performance(self, nav):
        result = performance(nav)
        assert result["Max Drawdown"] == pytest.approx(3.9885756705666631, 1e-10)

    def test_fee(self, nav):
        x = nav.fee(0.5)
        assert x[x.index[-1]] == pytest.approx(0.99454336215760819, 1e-10)

        x = nav.fee(0.0)
        assert x[x.index[-1]] == pytest.approx(1.0116455798589048, 1e-10)

    def test_monthly(self, nav):
        assert nav.monthly[pd.Timestamp("2014-11-30")] == pytest.approx(0.9902211463174124, 1e-10)

    def test_annual(self, nav):
        assert nav.annual[pd.Timestamp("2014-12-31")] == pytest.approx(0.9901407168626069, 1e-10)

    def test_weekly(self, nav):
        assert len(nav.weekly.index) == 70

    def test_annual_returns(self, nav):
        x = nav.returns_annual
        assert x[2014] == pytest.approx(-0.009859283137393149, 1e-7)
        assert x[2015] == pytest.approx(+0.021718996734564122, 1e-7)

    def test_truncate(self, nav):
        x = nav.truncate(before="2015-01-01")
        assert x.index[0] == pd.Timestamp("2015-01-01")

    def test_fromNav(self, ts):
        x = fromNav(ts)
        pdt.assert_series_equal(x.series, ts)

        x = fromNav(ts=None)
        pdt.assert_series_equal(x.series, pd.Series({}))

        with pytest.raises(AssertionError):
            # you can't set a negative Nav value:
            fromNav(ts=pd.Series(data=[1, 2, -10]))

    def test_periods(self, nav):
        p = nav.period_returns
        assert p.loc["Three Years"] == pytest.approx(0.011645579858904798, 1e-10)

    def test_drawdown_periods(self, nav):
        p = nav.drawdown_periods
        assert p.loc[pd.Timestamp("2014-03-07").date()] == pd.Timedelta(days=66)

    def test_with_dates(self):
        a = pd.Series({pd.Timestamp("2010-01-05").date(): 2.0,
                       pd.Timestamp("2012-02-13").date(): 3.0,
                       pd.Timestamp("2012-02-14").date(): 4.0
                       })

        n = fromNav(a)

        # no return in Jan 2012, 100% in Feb (from 2.0 to 4.0)
        pdt.assert_series_equal(n.ytd_series, pd.Series({pd.Timestamp("2012-02-14"): 1.0}))

        # we made 100% in Feb
        assert n.mtd == 1.0
        assert n.ytd == 1.0

        pdt.assert_series_equal(n.mtd_series, pd.Series({pd.Timestamp("2012-02-14"): 4.0/3.0 - 1.0, pd.Timestamp("2012-02-13"): 0.5}))

    def test_adjust(self):
        n = fromNav(pd.Series({}))
        assert n.adjust().empty

    def test_sortino_ratio_no_drawdown(self):
        x = pd.Series({pd.Timestamp("2012-02-13"): 1.0, pd.Timestamp("2012-02-14"): 1.0})
        n = fromNav(x)

        assert n.sortino_ratio() == np.inf

    def test_recent(self, nav):
        pdt.assert_series_equal(nav.recent(2), nav.pct_change().tail(2))

    def test_short(self):
        n = fromNav(ts=pd.Series({pd.Timestamp("30-Nov-2016"): 112}))
        assert n.periods_per_year == 256

    def test_from_returns(self):
        x = pd.Series(data=[0.0, 0.1, -0.1])
        r = fromReturns(x, adjust=True)
        pdt.assert_series_equal(r.series, pd.Series([1.0, 1.1, 0.99]))

        r = fromReturns(None, adjust=True)
        pdt.assert_series_equal(r.series, pd.Series({}))

    def test_to_frame(self, nav):
        frame = nav.to_frame(name="Maffay.")
        pdt.assert_series_equal(frame["Maffay.nav"], nav.series, check_names=False)
        pdt.assert_series_equal(frame["Maffay.drawdown"], nav.drawdown, check_names=False)
        frame = nav.to_frame()
        pdt.assert_series_equal(frame["nav"], nav.series, check_names=False)
        pdt.assert_series_equal(frame["drawdown"], nav.drawdown, check_names=False)

    def test_ewm_volatility(self, nav):
        x = nav.ewm_volatility(periods=256)
        pdt.assert_series_equal(x, 16 * nav.returns.fillna(0.0).ewm(com=50, min_periods=50).std(bias=False))

