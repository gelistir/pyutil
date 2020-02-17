import calendar
from collections import OrderedDict
from datetime import date

import numpy as np
import pandas as pd

from pyutil.performance.periods import period_returns


def drawdown(rseries) -> pd.Series:
    """
    Compute the drawdown for a price series. The drawdown is defined as 1 - price/highwatermark.
    The highwatermark at time t is the highest price that has been achieved before or at time t.

    Args:
        series:

    Returns: Drawdown as a pandas series
    """
    return Drawdown(rseries).drawdown


class Drawdown(object):
    def __init__(self, rseries: pd.Series, eps: float = 0) -> object:
        """
        Drawdown for a given series
        :param series: pandas Series
        :param eps: a day is down day if the drawdown (positive) is larger than eps
        """
        # check series is indeed a series
        assert isinstance(rseries, pd.Series)
        # check that all indices are increasing
        assert rseries.index.is_monotonic_increasing
        # make sure all entries non-negative
        # assert not (series < 0).any()

        self.__series = (rseries + 1.0).cumprod()
        self.__eps = eps

    @property
    def eps(self):
        return self.__eps

    @property
    def price_series(self) -> pd.Series:
        return self.__series

    @property
    def highwatermark(self) -> pd.Series:
        x = self.__series.expanding(min_periods=1).max()
        x[x <= 1.0] = 1.0
        return x

    @property
    def drawdown(self) -> pd.Series:
        return 1 - self.__series / self.highwatermark

    @property
    def periods(self):
        d = self.drawdown

        # the first price can not be in drawdown
        assert d.iloc[0] == 0

        # Drawdown days
        is_down = d > self.__eps

        s = pd.Series(index=is_down.index[1:], data=[r for r in zip(is_down[:-1], is_down[1:])])

        # move from no-drawdown to drawdown
        start = list(s[s == (False, True)].index)

        # move from drawdown to drawdown
        end = list(s[s == (True, False)].index)

        # eventually append the very last day...
        if len(end) < len(start):
            # add a point to the series... value doesn't matter
            end.append(s.index[-1])

        return pd.Series({s: e - s for s, e in zip(start, end)})

#
# def var(series, alpha=0.99):
#     return VaR(series, alpha).var
#
#
# def cvar(series, alpha=0.99):
#     return VaR(series, alpha).cvar
#

class VaR(object):
    def __init__(self, rseries, alpha=0.99):
        self.__series = rseries.dropna()
        self.__alpha = alpha

    @property
    def __losses(self):
        return self.__series * (-1)

    @property
    def __tail(self):
        losses = self.__losses
        return np.sort(losses.values)[int(len(losses) * self.alpha):]

    @property
    def cvar(self):
        return self.__tail.mean()

    @property
    def var(self):
        return self.__tail[0]

    @property
    def alpha(self):
        return self.__alpha


def monthlytable(r) -> pd.DataFrame:
    """
    Get a table of monthly returns

    :param nav:

    :return:
    """

    def _cumulate(x):
        return (1 + x).prod() - 1.0

    # r = nav.pct_change().dropna()
    # Works better in the first month
    # Compute all the intramonth-returns, instead of reapplying some monthly resampling of the NAV
    return_monthly = r.groupby([r.index.year, r.index.month]).apply(_cumulate)
    frame = return_monthly.unstack(level=1).rename(columns=lambda x: calendar.month_abbr[x])
    ytd = frame.apply(_cumulate, axis=1)
    frame["STDev"] = np.sqrt(12) * frame.std(axis=1)
    # make sure that you don't include the column for the STDev in your computation
    frame["YTD"] = ytd
    frame.index.name = "Year"
    frame.columns.name = None
    # most recent years on top
    return frame.iloc[::-1]


def from_nav(nav):
    return ReturnSeries(nav.dropna().pct_change().fillna(0.0))




class ReturnSeries(pd.Series):
    def __init__(self, *args, **kwargs):
        super(ReturnSeries, self).__init__(*args, **kwargs)

        if not self.empty:
            # change to DateTime
            if isinstance(self.index[0], date):
                self.rename(index=lambda x: pd.Timestamp(x), inplace=True)

            # check that all indices are increasing
            assert self.index.is_monotonic_increasing

    @property
    def series(self) -> pd.Series:
        return pd.Series({t: v for t, v in self.items()})

    @property
    def nav(self):
        return (self + 1.0).cumprod()

    @property
    def periods_per_year(self):
        if len(self.index) >= 2:
            x = pd.Series(data=self.index)
            return np.round(365 * 24 * 60 * 60 / x.diff().mean().total_seconds(), decimals=0)
        else:
            return 256

    @property
    def annual_returns(self):
        return (self + 1.0).groupby(self.index.year).prod() - 1.0

    @property
    def monthly_returns(self):
        return (self + 1.0).groupby([self.index.year, self.index.month]).prod() - 1.0

    @property
    def tail_month(self):
        first_day_of_month = (self.index[-1] + pd.offsets.MonthBegin(-1)).date()
        return self.truncate(before=first_day_of_month)

    @property
    def tail_year(self):
        first_day_of_year = (self.index[-1] + pd.offsets.YearBegin(-1)).date()
        return self.truncate(before=first_day_of_year)
    
    def resample(self, rule, **kwargs):
        return (self + 1.0).resample(rule=rule).prod() - 1.0

    @property
    def positive_events(self):
        return (self >= 0).sum()

    @property
    def negative_events(self):
        return (self < 0).sum()

    @property
    def events(self):
        return self.size

    @staticmethod
    def __gmean(a):
        # geometric mean A
        # Prod [a_i] == A^n
        # Apply log on both sides
        # Sum [log a_i] = n log A
        # => A = exp(Sum [log a_i] // n)
        return np.exp(np.mean(np.log(a)))

    def recent(self, n=15) -> pd.Series:
        return self.tail(n).dropna()

    # @property
    # def ytd_series(self) -> pd.Series:
    #     """
    #     Extract the series of monthly returns in the current year
    #     :return:
    #     """
    #     return self.__ytd(self.monthly_returns, today=self.index[-1]).sort_index(ascending=False)

    #@property
    #def mtd_series(self) -> pd.Series:
    #     return self.tail_month.sort_index(ascending=False)

    def var(self, alpha=0.95):
        return VaR(rseries=self, alpha=alpha).var

    def cvar(self, alpha=0.95):
        return VaR(rseries=self, alpha=alpha).cvar

    def truncate(self, before=None, after=None, axis=None, copy=True):
        return ReturnSeries(super().truncate(before=before, after=after, axis=axis, copy=copy))

    #def ytd(self, today=None) -> pd.Series:
    #    """
    #    Extract year to date of a series or a frame
#
#        :param ts: series or frame
#        :param today: today, relevant for testing
#
#         :return: ts or frame starting at the first day of today's year
#         """
#         today = today or pd.Timestamp("today")
#         first_day_of_year = (today + pd.offsets.YearBegin(-1)).date()
#         return ReturnSeries(self.truncate(before=first_day_of_year))

    #def mtd(self, today=None) -> pd.Series:
    #    today = today or pd.Timestamp("today")
    #    first_day_of_month = (today + pd.offsets.MonthBegin(-1)).date()
    #    return ReturnSeries(self.truncate(before=first_day_of_month))

    def ewm_volatility(self, com=50, min_periods=50, periods=None):
        periods = periods or self.periods_per_year
        return np.sqrt(periods) * self.ewm(com=com, min_periods=min_periods).std(bias=False)

    @property
    def monthlytable(self) -> pd.DataFrame:
        return monthlytable(self)

    @property
    def drawdown(self) -> pd.Series:
        return drawdown(rseries=self)

    @property
    def period_returns(self):
        # check series is indeed a series
        #assert isinstance(returns, pd.Series)
        # check that all indices are increasing
        #assert returns.index.is_monotonic_increasing
        # make sure all entries non-negative
        # assert not (prices < 0).any()

        return period_returns(returns=self, today=self.index[-1])

    def to_frame(self, name=""):
        frame = self.nav.to_frame("{name}nav".format(name=name))
        frame["{name}drawdown".format(name=name)] = self.drawdown
        return frame

    def annualized_volatility(self, periods=None):
        t = periods or self.periods_per_year
        return np.sqrt(t) * self.dropna().std()

    def sharpe_ratio(self, periods=None, r_f=0):
        return self.__mean_r(periods, r_f=r_f) / self.annualized_volatility(periods)

    def __mean_r(self, periods=None, r_f=0):
        # annualized performance over a risk_free rate r_f (annualized)
        periods = periods or self.periods_per_year
        return periods * (self.__gmean(self + 1.0) - 1.0) - r_f

    def sortino_ratio(self, periods=None, r_f=0):
        periods = periods or self.periods_per_year
        # cover the unrealistic case of 0 drawdown
        m = self.drawdown.max()
        if m == 0:
            return np.inf
        else:
            return self.__mean_r(periods, r_f=r_f) / m

    def calmar_ratio(self, periods=None, r_f=0):
        periods = periods or self.periods_per_year
        start = self.index[-1] - pd.DateOffset(years=3)
        # truncate the nav
        x = self.truncate(before=start)
        return ReturnSeries(x).sortino_ratio(periods=periods, r_f=r_f)

    def summary_format(self, alpha=0.95, periods=None, r_f=0):
        print("Hello")
        perf = self.summary(alpha=alpha, periods=periods, r_f=r_f)
        print(perf)

        f = lambda x: "{0:.2f}%".format(float(x))
        for name in ["Return", "Annua Return", "Annua Volatility", "Max Drawdown", "Max % return", "Min % return",
                     "MTD", "YTD", "Current Drawdown", "Value at Risk (alpha = 95)", "Conditional Value at Risk (alpha = 95)"]:
            perf[name] = f(perf[name])

        f = lambda x: "{0:.2f}".format(float(x))
        for name in ["Annua Sharpe Ratio (r_f = 0)", "Calmar Ratio (3Y)", "Current Nav", "Max Nav", "Kurtosis"]:
            perf[name] = f(perf[name])

        f = lambda x: "{:d}".format(int(x))
        for name in ["# Events", "# Events per year", "# Positive Events", "# Negative Events"]:
            perf[name] = f(perf[name])

        return perf


    # @property
    # def mtd(self):
    #     from pyutil.timeseries.aux import mtd as mm
    #     mm(ts=self)

    def summary(self, alpha=0.95, periods=None, r_f=0):
        periods = periods or self.periods_per_year

        d = OrderedDict()
        # d = Dict()

        d["Return"] = 100 * ((self + 1).cumprod() - 1.0).tail(1).values[0]
        d["# Events"] = self.events
        d["# Events per year"] = periods

        d["Annua Return"] = 100 * self.__mean_r(periods=periods)
        d["Annua Volatility"] = 100 * self.annualized_volatility(periods=periods)
        d["Annua Sharpe Ratio (r_f = {0})".format(r_f)] = self.sharpe_ratio(periods=periods, r_f=r_f)

        #dd = self.drawdown
        #d["Max Drawdown"] = 100 * dd.max()
        d["Max % return"] = 100 * self.max()
        d["Min % return"] = 100 * self.min()

        print(d)
        # d["MTD"] = 100 * self.mtd
        # d["YTD"] = 100 * self.ytd
        #
        # d["Current Nav"] = self.tail(1).values[0]
        # d["Max Nav"] = self.max()
        # #d["Current Drawdown"] = 100 * dd[dd.index[-1]]
        #
        d["Calmar Ratio (3Y)"] = self.calmar_ratio(periods=periods, r_f=r_f)
        #
        d["# Positive Events"] = self.positive_events
        d["# Negative Events"] = self.negative_events
        #
        d["Value at Risk (alpha = {alpha})".format(alpha=int(100 * alpha))] = 100 * self.var(alpha=alpha)
        d["Conditional Value at Risk (alpha = {alpha})".format(alpha=int(100 * alpha))] = 100 * self.cvar(alpha=alpha)
        d["First at"] = self.index[0].date()
        d["Last at"] = self.index[-1].date()
        d["Kurtosis"] = self.kurtosis()

        x = pd.Series(d)
        x.index.name = "Performance number"
        print(x)
        return x


# if __name__ == '__main__':
#     t1 = pd.Timestamp("2010-10-21")
#     t2 = pd.Timestamp("2010-10-22")
#     t3 = pd.Timestamp("2010-10-23")
#     r = pd.Series(index=[t1, t2, t3], data=[-0.01, 0.02, -0.01])
#     xxx = ReturnSeries(r)
#     print(xxx.drawdown)
#     print(xxx.monthlytable)
#     print(xxx.to_frame())
#     print(xxx.annualized_volatility(periods=256))
#     print(xxx.monthly_returns)
#     print(xxx.tail_month)
#     print(xxx.tail_year)



