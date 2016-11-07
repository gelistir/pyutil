import pandas as pd
import numpy as np

from pyutil.performance.performance import performance
from pyutil.performance.drawdown import drawdown as dd
from pyutil.performance.month import monthlytable
from pyutil.performance.periods import period_returns, periods



class _Summary(object):
    @staticmethod
    def __gmean(a):
        # geometric mean A
        # Prod [a_i] == A^n
        # Apply log on both sides
        # Sum [log a_i] = n log A
        # => A = exp(Sum [log a_i] // n)
        return np.exp(np.mean(np.log(a)))


    def __init__(self, ts):
        self.__ts = ts

    @property
    def first(self):
        return self.__ts[0]

    @property
    def last(self):
        return self.__ts[-1]

    @property
    def positive_days(self):
        return (self.__ts > 0).sum()

    @property
    def negative_days(self):
        return (self.__ts < 0).sum()

    @property
    def max_r(self):
        return self.__ts.max()

    @property
    def min_r(self):
        return self.__ts.min()

    @property
    def days(self):
        return self.__ts.size

    @property
    def std(self):
        return self.__ts.std()

    @property
    def cum_return(self):
        return (1 + self.__ts).prod() - 1.0

    @property
    def sharpe(self):
        geo_mean = self.__gmean(self.__ts + 1.0)  - 1.0
        return geo_mean/self.std

    #@property
    #def drawdown(self):
    #    nav = (1 + self.__ts).cumprod()
    #    return dd(nav)

    @property
    def nav(self):
        return (1 + self.__ts).cumprod()

    #@property
    #def current_nav(self):
    #    return self.nav[self.nav.index[-1]]


    #d["Annua. Return"] = annualized_return(nav, days=days)
    #d["Annua. Volatility"] = annualized_volatility(nav, days=days)

    #per = period_returns(returns=r, offset=periods(today=r.index[-1]))

    #d["MTD"] = 100*per["Month-to-Date"]
    #d["YTD"] = 100*per["Year-to-Date"]


    #d["Calmar Ratio (3Y)"] = sortino_ratio(nav, days=days)

    #d["Value at Risk (alpha = 0.95)"] = 100*value_at_risk(nav.dropna(), alpha=0.95)
    #d["Conditional Value at Risk (alpha = 0.95)"] = 100*conditional_value_at_risk(nav.dropna(), alpha=0.95)

def fromReturns(r):
    return Nav((1 + r).cumprod())

def fromNav(ts):
    return Nav(ts)

class Nav(object):
    def __init__(self, ts):
        self.__nav = ts

    @property
    def monthlytable(self):
        return monthlytable(self.__nav)

    def summary(self, days=262):
        d = dict()
        d["all"] = self.performance(days)
        for n in [100, 250, 500, 1000, 1500, 2500]:
            d[n] = self.tail(n).performance(days)

        return pd.DataFrame(d)

    @property
    def drawdown(self):
        return dd(self.__nav)

    def ewm_volatility(self, com=50, min_periods=50, days=262):
        return np.sqrt(days) * self.returns.fillna(0.0).ewm(com=com, min_periods=min_periods).std(bias=False)

    def ewm_ret(self, com=50, min_periods=50, days=262):
        return days * self.returns.fillna(0.0).ewm(com=com, min_periods=min_periods).mean()

    def ewm_sharpe(self, com=50, min_periods=50, days=262):
        return self.ewm_ret(com, min_periods, days) / self.ewm_volatility(com, min_periods, days)

    def adjust(self, value=100):
        x = self.__nav.dropna()
        f = value / x[x.index[0]]
        return Nav(self.__nav * f)

    def performance(self, days=262):
        return performance(self.__nav, days).ix[
            ["Annua. Return", "Annua. Volatility", "Annua. Sharpe Ratio", "Calmar Ratio (3Y)", "Max Nav",
             "Max Drawdown", "YTD", "MTD", "Current Nav", "Current Drawdown", "Positive Days", "Negative Days"]]

    def tail(self, n):
        return Nav(self.__nav.tail(n))

    @property
    def series(self):
        return self.__nav

    @property
    def returns(self):
        return self.__nav.dropna().pct_change().fillna(0.0)

    def truncate(self, before=None, after=None):
        return Nav(self.__nav.truncate(before, after))

    def period_returns(self):
        n = self.__nav.pct_change()
        return period_returns(n, periods(today=n.index[-1]))

    def fee(self, daily_fee_basis_pts=0.5):
        ret = self.returns.fillna(0.0) - daily_fee_basis_pts / 10000.0
        return Nav((ret + 1.0).cumprod())

    @property
    def losses(self):
        return self.__nav.pct_change().dropna() * (-1)

    @property
    def monthly(self):
        return Nav(self.__nav.resample("M").last())

    @property
    def annual(self):
        return Nav(self.__nav.resample("A").last())

    def __getitem__(self, item):
        return self.__nav[item]

    @property
    def statistics(self):
        return _Summary(self.returns)
