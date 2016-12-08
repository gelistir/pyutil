from collections import OrderedDict

import pandas as pd
import numpy as np

from .month import monthlytable
from .drawdown import drawdown as dd
from .periods import period_returns, periods
from .var import value_at_risk, conditional_value_at_risk


def performance(nav, alpha=0.95, periods=None):
    return Summary(nav).summary(alpha=alpha, periods=periods)


class Summary(object):
    @staticmethod
    def __gmean(a):
        # geometric mean A
        # Prod [a_i] == A^n
        # Apply log on both sides
        # Sum [log a_i] = n log A
        # => A = exp(Sum [log a_i] // n)
        return np.exp(np.mean(np.log(a)))

    @property
    def periods_per_year(self):
        x = pd.Series(data=self.__nav.index)
        return np.round(365 * 24 * 60 * 60 / x.diff().mean().total_seconds(), decimals=0)

    def __init__(self, nav):
        self.__nav = nav
        self.__r = nav.pct_change().dropna()

    @property
    def series(self):
        return self.__nav

    @property
    def monthlytable(self):
        return monthlytable(self.__nav)

    @property
    def first(self):
        return self.__nav.values[0]

    @property
    def last(self):
        return self.__nav.values[-1]

    @property
    def positive_events(self):
        return (self.__r >= 0).sum()

    @property
    def negative_events(self):
        return (self.__r < 0).sum()

    @property
    def max_r(self):
        return self.__r.max()

    @property
    def min_r(self):
        return self.__r.min()

    @property
    def max_nav(self):
        return self.__nav.max()

    @property
    def min_nav(self):
        return self.__nav.min()

    @property
    def events(self):
        return self.__r.size

    def std(self, periods=None):
        periods = periods or self.periods_per_year
        return np.sqrt(periods)*self.__r.std()

    @property
    def cum_return(self):
        return (1 + self.__r).prod() - 1.0

    def sharpe_ratio(self, periods=None):
        return self.mean_r(periods)/self.std(periods)

    def mean_r(self, periods=None):
        periods = periods or self.periods_per_year
        return periods*(self.__gmean(self.__r + 1.0)  - 1.0)

    @property
    def drawdown(self):
        return dd(self.__nav)

    def sortino_ratio(self, periods=None):
        periods = periods or self.periods_per_year
        return self.mean_r(periods) / self.drawdown.max()

    def calmar_ratio(self, periods=None):
        periods = periods or self.periods_per_year
        start = self.__nav.index[-1] - pd.DateOffset(years=3)
        # truncate the nav
        x = self.__nav.truncate(before=start)
        return Summary(x).sortino_ratio(periods=periods)

    @property
    def autocorrelation(self):
        """
        Compute the autocorrelation of returns
        :return:
        """
        return self.__r.autocorr(lag=1)

    @property
    def mtd(self):
        """
        Compute the return in the last available month
        :return:
        """
        return self.__nav.resample("M").last().dropna().pct_change().tail(1).values[0]

    @property
    def ytd(self):
        """
        Compute the return in the last available year
        :return:
        """
        return self.__nav.resample("A").last().dropna().pct_change().tail(1).values[0]

    def var(self, alpha=0.95):
        return value_at_risk(self.__nav, alpha=alpha)

    def cvar(self, alpha=0.95):
        return conditional_value_at_risk(self.__nav, alpha=alpha)

    def summary(self, alpha=0.95, periods=None):
        periods = periods or self.periods_per_year

        d = OrderedDict()

        d["Return"] = 100 * self.cum_return
        d["# Events"] = self.events
        d["# Events per year"] = periods

        d["Annua. Return"] = 100 * self.mean_r(periods=periods)
        d["Annua. Volatility"] = 100 * self.std(periods=periods)
        d["Annua. Sharpe Ratio"] = self.sharpe_ratio(periods=periods)

        dd = self.drawdown
        d["Max Drawdown"] = 100 * dd.max()
        d["Max % return"] = 100 * self.max_r
        d["Min % return"] = 100 * self.min_r

        d["MTD"] = 100*self.mtd
        d["YTD"] = 100*self.ytd

        d["Current Nav"] = self.__nav.tail(1).values[0]
        d["Max Nav"] = self.max_nav
        d["Current Drawdown"] = 100 * dd[dd.index[-1]]

        d["Calmar Ratio (3Y)"] = self.calmar_ratio(periods=periods)

        d["# Positive Events"] = self.__r[self.__r > 0].size
        d["# Negative Events"] = self.__r[self.__r < 0].size
        d["Value at Risk (alpha = {alpha})".format(alpha=alpha)] = 100*self.var(alpha=alpha)
        d["Conditional Value at Risk (alpha = {alpha})".format(alpha=alpha)] = 100*self.cvar(alpha=alpha)
        d["First"] = self.__nav.index[0].date()
        d["Last"] = self.__nav.index[-1].date()

        return pd.Series(d)

    def ewm_volatility(self, com=50, min_periods=50, periods=None):
        periods = periods or self.periods_per_year
        return np.sqrt(periods) * self.__r.fillna(0.0).ewm(com=com, min_periods=min_periods).std(bias=False)

    def ewm_ret(self, com=50, min_periods=50, periods=None):
        periods = periods or self.periods_per_year
        return periods * self.__r.fillna(0.0).ewm(com=com, min_periods=min_periods).mean()

    def ewm_sharpe(self, com=50, min_periods=50, periods=None):
        periods = periods or self.periods_per_year
        return self.ewm_ret(com, min_periods, periods) / self.ewm_volatility(com, min_periods, periods)

    @property
    def period_returns(self):
        return period_returns(self.__r, periods(today=n.index[-1]))
