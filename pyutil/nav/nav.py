import pandas as pd
import numpy as np

from pyutil.performance.performance import performance
from pyutil.performance.drawdown import drawdown
from pyutil.performance.month import monthlytable
from pyutil.performance.periods import period_returns, periods


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
        return drawdown(self.__nav)

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
        return self.__nav.dropna().pct_change()

    def truncate(self, before, after=None):
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
