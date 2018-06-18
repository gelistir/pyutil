from collections import OrderedDict
from datetime import date

import pandas as pd
import numpy as np

from pyutil.timeseries.timeseries import mtd, ytd
from .month import monthlytable
from .drawdown import drawdown as dd, drawdown_periods as dp
from .periods import period_returns
from .var import value_at_risk, conditional_value_at_risk


def fromReturns(r):
    return NavSeries((1 + r).cumprod().dropna()).adjust(value=1.0)


def fromNav(ts, adjust=True):
    x = NavSeries(ts.dropna())
    if adjust:
        return x.adjust(value=1.0)

    return x

def performance(nav, alpha=0.95, periods=None):
    return NavSeries(nav.dropna()).summary(alpha=alpha, periods=periods)


class NavSeries(pd.Series):
    def __init__(self, *args, **kwargs):
        super(NavSeries, self).__init__(*args, **kwargs)
        if not self.empty:
            if isinstance(self.index[0], date):
                self.rename(index=lambda x: pd.Timestamp(x), inplace=True)

    @property
    def __periods_per_year(self):
        if len(self.index) >= 2:
            x = pd.Series(data=self.index)
            return np.round(365 * 24 * 60 * 60 / x.diff().mean().total_seconds(), decimals=0)
        else:
            return 256

    def annualized_volatility(self, periods=None):
        t = periods or self.__periods_per_year
        return np.sqrt(t)*self.dropna().pct_change().std()

    @staticmethod
    def __gmean(a):
        # geometric mean A
        # Prod [a_i] == A^n
        # Apply log on both sides
        # Sum [log a_i] = n log A
        # => A = exp(Sum [log a_i] // n)
        return np.exp(np.mean(np.log(a)))

    def truncate(self, before=None, after=None):
        return NavSeries(super().truncate(before=before, after=after))

    @property
    def monthlytable(self):
        return monthlytable(self)

    @property
    def returns(self):
        return self.pct_change().dropna()

    @property
    def returns_monthly(self):
        return self.monthly.pct_change().dropna()

    @property
    def returns_annual(self):
        x = self.annual.pct_change().dropna()
        x.index = [a.year for a in x.index]
        return x

    @property
    def positive_events(self):
        return (self.returns >= 0).sum()

    @property
    def negative_events(self):
        return (self.returns < 0).sum()

    @property
    def events(self):
        return self.returns.size

    @property
    def cum_return(self):
        return (1 + self.returns).prod() - 1.0

    def sharpe_ratio(self, periods=None, r_f=0):
        return self.mean_r(periods, r_f=r_f) /self.annualized_volatility(periods)

    def mean_r(self, periods=None, r_f=0):
        # annualized performance over a risk_free rate r_f (annualized)
        periods = periods or self.__periods_per_year
        return periods*(self.__gmean(self.returns + 1.0)  - 1.0) - r_f

    @property
    def drawdown(self):
        return dd(self)

    def sortino_ratio(self, periods=None, r_f=0):
        periods = periods or self.__periods_per_year
        # cover the unrealistic case of 0 drawdown
        m = self.drawdown.max()
        if m == 0:
            return np.inf
        else:
            return self.mean_r(periods, r_f=r_f) / m

    def calmar_ratio(self, periods=None, r_f=0):
        periods = periods or self.__periods_per_year
        start = self.index[-1] - pd.DateOffset(years=3)
        # truncate the nav
        x = self.truncate(before=start)
        return NavSeries(x).sortino_ratio(periods=periods, r_f=r_f)

    @property
    def autocorrelation(self):
        """
        Compute the autocorrelation of returns
        :return:
        """
        return self.returns.autocorr(lag=1)

    @property
    def mtd(self):
        """
        Compute the return in the last available month, note that you need at least one point in the previous month, too. Otherwise Last/First - 1
        :return:
        """
        x = self.resample("M").last().dropna()

        if len(x) <= 1:
            return self.dropna().tail(1).values[0]/self.dropna().head(1).values[0] - 1.0
        else:
            return x.pct_change().dropna().tail(1).values[0]

    @property
    def ytd(self):
        """
        Compute the return in the last available year, note that you need at least one point in the previous year, too. Otherwise Last/First - 1
        :return:
        """
        x = self.resample("A").last().dropna()

        if len(x) <= 1:
            return self.dropna().tail(1).values[0]/self.dropna().head(1).values[0] - 1.0
        else:
            return x.pct_change().dropna().tail(1).values[0]

    @property
    def mtd_series(self):
        """
        Extract the series of returns in the current month
        :return:
        """
        return mtd(self.returns, today=self.index[-1])

    @property
    def ytd_series(self):
        """
        Extract the series of monthly returns in the current year
        :return:
        """
        return ytd(self.returns_monthly, today=self.index[-1])


    def recent(self, n=15):
        return self.pct_change().tail(n).dropna()

    def var(self, alpha=0.95):
        return value_at_risk(self, alpha=alpha)

    def cvar(self, alpha=0.95):
        return conditional_value_at_risk(self, alpha=alpha)

    def summary(self, alpha=0.95, periods=None, r_f=0):
        periods = periods or self.__periods_per_year

        d = OrderedDict()

        d["Return"] = 100 * self.cum_return
        d["# Events"] = self.events
        d["# Events per year"] = periods

        d["Annua Return"] = 100 * self.mean_r(periods=periods)
        d["Annua Volatility"] = 100 * self.annualized_volatility(periods=periods)
        d["Annua Sharpe Ratio (r_f = {0})".format(r_f)] = self.sharpe_ratio(periods=periods, r_f=r_f)

        dd = self.drawdown
        d["Max Drawdown"] = 100 * dd.max()
        d["Max % return"] = 100 * self.returns.max()
        d["Min % return"] = 100 * self.returns.min()

        d["MTD"] = 100*self.mtd
        d["YTD"] = 100*self.ytd

        d["Current Nav"] = self.tail(1).values[0]
        d["Max Nav"] = self.max()
        d["Current Drawdown"] = 100 * dd[dd.index[-1]]

        d["Calmar Ratio (3Y)"] = self.calmar_ratio(periods=periods, r_f=r_f)

        d["# Positive Events"] = self.positive_events
        d["# Negative Events"] = self.negative_events

        d["Value at Risk (alpha = {alpha})".format(alpha=int(100*alpha))] = 100*self.var(alpha=alpha)
        d["Conditional Value at Risk (alpha = {alpha})".format(alpha=int(100*alpha))] = 100*self.cvar(alpha=alpha)
        d["First_at"] = self.index[0].date()
        d["Last_at"] = self.index[-1].date()

        x = pd.Series(d)
        x.index.name = "Performance number"
        return x

    def ewm_volatility(self, com=50, min_periods=50, periods=None):
        periods = periods or self.__periods_per_year
        return np.sqrt(periods) * self.returns.fillna(0.0).ewm(com=com, min_periods=min_periods).std(bias=False)

    def ewm_ret(self, com=50, min_periods=50, periods=None):
        periods = periods or self.__periods_per_year
        return periods * self.returns.fillna(0.0).ewm(com=com, min_periods=min_periods).mean()

    def ewm_sharpe(self, com=50, min_periods=50, periods=None):
        periods = periods or self.__periods_per_year
        return self.ewm_ret(com, min_periods, periods) / self.ewm_volatility(com, min_periods, periods)

    @property
    def period_returns(self):
        return period_returns(self.returns, today=self.index[-1])

    def adjust(self, value=100.0):
        if self.empty:
            return NavSeries(pd.Series({}))
        else:
            first = self[self.dropna().index[0]]
            return NavSeries(self * value / first)

    @property
    def monthly(self):
        return NavSeries(self.__res("M"))

    @property
    def annual(self):
        return NavSeries(self.__res("A"))

    @property
    def weekly(self):
        return NavSeries(self.__res("W"))

    def fee(self, daily_fee_basis_pts=0.5):
        ret = self.pct_change().fillna(0.0) - daily_fee_basis_pts / 10000.0
        return NavSeries((ret + 1.0).cumprod())

    @property
    def drawdown_periods(self):
        return dp(self)

    #@property
    #def annual_returns(self):
    #    x = self.annual.pct_change().dropna()
    #    x.index = [a.year for a in x.index]
    #    return x

    def __res(self, rule="M"):
        ### refactor NAV at the end but keep the first element. Important for return computations!

        a = pd.concat((self.head(1), self.resample(rule).last()), axis=0)
        # overwrite the last index with the trust last index
        a.index = a.index[:-1].append(pd.DatetimeIndex([self.index[-1]]))
        return a

    #def to_dictionary(self, **kwargs):
    #    return {**{"nav": series2array(self),
    #               "drawdown": series2array(self.drawdown),
    #               "volatility": series2array(self.ewm_volatility())}, **kwargs}


