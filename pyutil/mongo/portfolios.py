import pandas as pd

from pyutil.performance.periods import periods as pp, period_returns as pr
from pyutil.portfolio.portfolio import Portfolio
from pyutil.timeseries.timeseries import ytd as yy, mtd as mm, adjust


class Portfolios(object):
    """
    Dictionary of Portfolios
    """

    def __init__(self):
        self.__portfolio = dict()

    def __getitem__(self, item):
        # get a particular asset
        return self.__portfolio[item]

    @property
    def empty(self):
        return len(self) == 0

    def __len__(self):
        return len(self.__portfolio)

    def __setitem__(self, key, value):
        assert isinstance(value, Portfolio)
        self.__portfolio[key] = value

    def keys(self):
        return self.__portfolio.keys()

    def items(self):
        for k in self.keys():
            yield (k, self[k])

    def nav(self, before=None):
        return pd.DataFrame({name: portfolio.nav for name, portfolio in self.items()}).truncate(before=before).apply(adjust)

    @property
    def strategies(self):
        s = pd.DataFrame({name: portfolio.meta for name, portfolio in self.items()}).transpose()
        if "time" in s.keys():
            s["time"] = s["time"].apply(lambda t: t.strftime("%Y-%m-%d"))
        return s


    def __repr__(self):
        return str.join("\n", [str(self[portfolio]) for portfolio in self.keys()])

    def sector_weights(self, symbolmap):
        def f(x):
            return x.ix[x.index[-1]]

        return 100*pd.DataFrame({name: f(p.sector_weights(symbolmap)) for name, p in self.items()}).fillna(0.0).transpose()

    @staticmethod
    def __g(r, format):
        frame = r.transpose().sort_index(axis=1, ascending=False)
        frame.rename(columns=lambda x: x.strftime(format), inplace=True)
        frame["total"] = (frame.fillna(0.0) + 1.0).prod(axis=1) - 1.0
        return 100 * frame

    @property
    def mtd(self):
        r = self.nav().pct_change().dropna(axis=1, how="all")
        return self.__g(r.apply(mm, today=r.index[-1]), format="%b %d")

    def recent(self, n=15):
        r = self.nav().pct_change().tail(n)
        return self.__g(r, format="%b %d")

    @property
    def ytd(self):
        r = self.nav().resample("M").last().pct_change().dropna(axis=1, how="all")
        return self.__g(r.apply(yy, today=r.index[-1]), format="%b")

    @property
    def period_returns(self):
        frame = self.nav().pct_change()
        # compute the periods
        offset = pp(today=frame.index[-1])
        return 100*frame.apply(pr, offset=offset).transpose()


