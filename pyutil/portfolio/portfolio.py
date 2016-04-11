import pandas as pd
from pyutil.performance.periods import period_returns, periods
from pyutil.nav.nav import Nav
from pyutil.timeseries.timeseries import subsample


class Portfolio(object):
    def __init__(self, prices, weights):
        self.__prices = prices.ix[weights.index]
        self.__weights = weights

    def __repr__(self):
        return "Portfolio with assets: {0}".format(list(self.__weights.keys()))

    @property
    def cash(self):
        return 1.0 - self.weights.ffill().sum(axis=1)

    @property
    def assets(self):
        return self.__prices.keys()

    @property
    def prices(self):
        return self.__prices

    @property
    def weights(self):
        return self.__weights

    @property
    def asset_returns(self):
        return self.__prices.pct_change()

    @property
    def nav(self):
        return Nav((self.weighted_returns.sum(axis=1) + 1).cumprod().dropna())

    @property
    def weighted_returns(self):
        d = dict()
        for asset in self.assets:
            # the stream of returns starts with a zero!
            rr = self.prices[asset].dropna().pct_change().fillna(0.0)
            ww = self.weights[asset].dropna().shift(1).fillna(0.0)
            d[asset] = ww*rr

        return pd.DataFrame(d)

    @property
    def index(self):
        return self.prices.index

    @property
    def leverage(self):
        return self.weights.ffill().sum(axis=1).dropna()

    def summary(self, days=262):
        return pd.DataFrame({n: self.tail(n).performance(days) for n in [100, 250, 500, 1000, 1500, 2500, 5000]})

    def performance(self, days=262):
        l = self.leverage
        lev = pd.Series({"Av Leverage": l.mean(), "Current Leverage": l[l.index[-1]]})
        return pd.concat((self.nav.performance(days), lev))

    def truncate(self, before=None, after=None):
        return Portfolio(prices=self.prices.truncate(before=before, after=after),
                         weights=self.weights.truncate(before=before, after=after))

    @property
    def weight_current(self):
        w = self.weights.ffill()
        a = w.ix[w.index[-1]]
        a.index.name = "weight"
        return a

    def sector_weights(self, symbolmap):
        weights = self.weights.ffill()
        assets = weights.keys()
        groups = symbolmap[assets]
        frame = pd.DataFrame({group: weights[groups[groups == group].index].sum(axis=1) for group in groups.unique()})
        frame["total"] = frame.sum(axis=1)
        return frame

    def snapshot(self, day=15):
        today = self.index[-1]
        offsets = periods(today)

        a = self.weighted_returns.apply(period_returns, offset=offsets).transpose()[["Month-to-Date", "Year-to-Date"]]
        b = subsample(self.weights.ffill(), day=day).tail(5).rename(index=lambda x: x.strftime("%b %d")).transpose()
        return pd.concat((a, b), axis=1)

    def top_flop(self, day_final=pd.Timestamp("today")):
        d = dict()
        s = self.weighted_returns.apply(period_returns, offset=periods(today=day_final)).transpose()

        a = s.sort_values(by=["Month-to-Date"], ascending=True)[["Month-to-Date"]]
        d["flop MTD"] = a.head(5).reset_index().rename(columns={"Month-to-Date": "Value"})
        a = s.sort_values(by=["Month-to-Date"], ascending=False)[["Month-to-Date"]]
        d["top MTD"] = a.head(5).reset_index().rename(columns={"Month-to-Date": "Value"})
        a = s.sort_values(by=["Year-to-Date"], ascending=True)[["Year-to-Date"]]
        d["flop YTD"] = a.head(5).reset_index().rename(columns={"Year-to-Date": "Value"})
        a = s.sort_values(by=["Year-to-Date"], ascending=False)[["Year-to-Date"]]
        d["top YTD"] = a.head(5).reset_index().rename(columns={"Year-to-Date": "Value"})
        return pd.concat(d, axis=0)

    def tail(self, n=10):
        w = self.weights.tail(n)
        p = self.prices.ix[w.index]
        return Portfolio(p, w)

    @property
    def position(self):
        nav = self.nav.series
        return pd.DataFrame({k: self.weights[k] * nav / self.prices[k] for k in self.assets})

    @property
    def trades_relative(self):
        """
        :return: trades as fraction of a portfolio per asset and day
        """
        trade_in_usd = self.position.ffill().diff().fillna(0.0)*self.prices.ffill()
        n = self.nav.series
        return pd.DataFrame({key: trade_in_usd[key]/n for key in trade_in_usd.keys()})

    def subportfolio(self, assets):
        return Portfolio(prices=self.prices[assets], weights=self.weights[assets])

    def nav_adjusted(self, size=1e6, flatfee=0.0, basispoints=20, threshold=0.01):
        r0 = self.nav.returns
        r1 = self.trades_relative.abs().sum(axis=1)*basispoints / 10000
        r2 = self.trade_count(threshold).sum(axis=1)*flatfee / (self.nav.series * size)

        # make this a nav again
        r = r0 - r1 - r2
        return Nav((1 + r).cumprod())

    def trade_count(self, threshold=0.01):
        t = self.trades_relative.abs()
        t[t >= threshold] = 1
        t[t < threshold] = 0
        return t


