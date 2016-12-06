from pyutil.timeseries.timeseries import ytd, mtd, adjust
from pyutil.performance.summary import Summary


def fromReturns(r):
    return _Nav((1 + r).cumprod().dropna())

def fromNav(ts):
    return _Nav(ts)


class _Nav(object):
    def __init__(self, ts):
        self.__nav = ts

    @property
    def index(self):
        return self.__nav.index

    #def summary(self, periods=None):
    #    d = dict()
    #    d["all"] = self.performance(periods=periods)
    #    for n in [100, 250, 500, 1000, 1500, 2500]:
    #        d[n] = self.tail(n).performance(periods=periods)

    #    return pd.DataFrame(d)

    def adjust(self, value=100):
        return _Nav(adjust(self.__nav) * value)

    #def performance(self, periods=None):
    #    return performance(self.__nav, days).ix[
    #        ["Annua. Return", "Annua. Volatility", "Annua. Sharpe Ratio", "Calmar Ratio (3Y)", "Max Nav",
    #         "Max Drawdown", "YTD", "MTD", "Current Nav", "Current Drawdown", "Positive Days", "Negative Days"]]

    def tail(self, n):
        return _Nav(self.series.tail(n))

    @property
    def drawdown(self):
        return self.statistics.drawdown

    @property
    def series(self):
        return self.__nav

    @property
    def returns(self):
        return self.__nav.dropna().pct_change().fillna(0.0)

    def truncate(self, before=None, after=None):
        return _Nav(self.series.truncate(before, after))

    def fee(self, daily_fee_basis_pts=0.5):
        ret = self.returns.fillna(0.0) - daily_fee_basis_pts / 10000.0
        return _Nav((ret + 1.0).cumprod())

    @property
    def monthly(self):
        return self.resample("M")

    @property
    def annual(self):
        return self.resample("A")

    @property
    def weekly(self):
        return self.resample("W")

    @property
    def daily(self):
        return self.resample("D")

    def __getitem__(self, item):
        return self.__nav[item]

    @property
    def ytd(self):
        n = len(ytd(self.__nav, today=self.__nav.index[-1]))
        return self.tail(n+1)

    @property
    def mtd(self):
        n = len(mtd(self.__nav, today=self.__nav.index[-1]))
        return self.tail(n+1)

    @property
    def statistics(self):
        return Summary(self.series)

    def resample(self, frequency="D"):
        return _Nav(self.__nav.resample(frequency).last())

