import pandas as pd
from pyutil.performance.periods import period_returns, periods
from pyutil.nav.nav import Nav
from pyutil.timeseries.timeseries import subsample
from itertools import tee


# def build(prices, weights):
#     # make sure the weights are a subset of the prices
#     for time in weights.index:
#         assert time in prices.index
#
#     for asset in weights.keys():
#         assert asset in prices.keys()
#
#     # start with all positions are NaN
#     pos = pd.DataFrame(index=prices.index, columns=prices.keys())
#
#     cash = pd.Series(index=prices.index)
#
#     # set the cash for all the points in time we have weights
#     cash.ix[weights.index] = 1.0 - weights.sum(axis=1)
#
#     # set the position for the points in time we have weights
#     pos.ix[weights.index] = weights / prices[weights.keys()].ix[weights.index]
#
#     # compute the value of our positions for all points in time(!)
#     value = pos.ffill() * prices.ffill()
#
#     # the amount of cash won't change 'til the next reallocation
#     total = cash.ffill() + value.sum(axis=1)
#
#     # the weights are now no longer sparse
#     weights = pd.DataFrame({t: value.ix[t] / total[t] for t in value.index}).transpose()
#
#     return Portfolio(prices=prices, weights=weights)


# def build(prices, weights):
#     # make sure the weights are a subset of the prices
#     # for time in weights.index:
#      #   assert time in prices.index
#
#     #for asset in weights.keys():
#     #    assert asset in prices.keys()
#
#     w = pd.DataFrame(index=prices.index, columns=prices.keys())
#     t_first = weights.index[0]
#     w.ix[t_first] = weights.ix[t_first]
#
#     for t1, t2 in pairwise(prices.index[prices.index >= t_first]):
#         if t2 in weights.index:
#             w.ix[t2] = weights.ix[t2]
#         else:
#             w.ix[t2] = forward(w.ix[t1], prices.ix[t1], prices.ix[t2])
#
#     return Portfolio(prices=prices, weights=w)


def merge(portfolios, axis=0):
    prices = pd.concat([p.prices for p in portfolios], axis=axis, verify_integrity=True)
    weights = pd.concat([p.weights for p in portfolios], axis=axis, verify_integrity=True)
    return Portfolio(prices, weights)


class Portfolio(object):
    @staticmethod
    def __pairwise(iterable):
        "s -> (s0,s1), (s1,s2), (s2, s3), ..."
        a, b = tee(iterable)
        next(b, None)
        return zip(a, b)

    @staticmethod
    def __forward(w1, p1, p2):
        # w1 weight at time t1
        # p1 price at time t1
        # p2 price at time t2
        # return the weights at time t2
        import numpy as np
        cash = 1 - w1.sum()  # cash at time t1
        pos = w1 / p1  # pos at time t1
        value1 = (pos * p1).sum() + cash  # value at time t1
        assert np.absolute(1.0 - value1) < 0.05, "The value is {0}. Cash is {1}. Weights before {2}".format(np.absolute(1 - value1.sum()), cash, w1)
        value2 = pos * p2  # value at time t2
        w2 = value2 / (value2.sum() + cash)
        return w2.apply(float)

    def iron_threshold(self, threshold=0.02):
        """
        Iron a portfolio, do not touch the last index

        :param threshold:
        :return:
        """
        w = self.weights.copy().ffill()

        for t1, t2 in self.__pairwise(self.weights.ix[:-1].index):
            if (w.ix[t2] - w.ix[t1]).abs().max() > threshold:
                w.ix[t2] = self.weights.ix[t2]
            else:
                w.ix[t2] = self.__forward(w.ix[t1], self.prices.ix[t1], self.prices.ix[t2])

        return Portfolio(prices=self.prices, weights=w)

    def iron_time(self, rule):

        # take the weights of the underlying portfolio
        index = self.weights.index

        moments = [index[0]]

        for t in self.weights.ix[:-1].resample(rule=rule).last().index:
            # take the last stamp before or at time t.
            # Often t is a weekend and we have no trading on this particular day
            moments.append(index[index <= t][-1])

        moments.append(self.weights.index[-1])
        return Portfolio(prices=self.prices, weights=self.weights.ix[moments])

    def __init__(self, prices, weights):
        # make sure the weights are a subset of the prices
        for time in weights.index:
            assert time in prices.index

        for asset in weights.keys():
            assert asset in prices.keys()

        # only take into account prices after the first weight!
        t_first = weights.index[0]
        p = prices.ffill().ix[prices.index >= t_first]

        # set the first weight
        w = pd.DataFrame(index=p.index, columns=p.keys(), data=0.0)

        # the weights given are set
        w.ix[weights.index] = weights

        # loop over all times
        for t1, t2 in self.__pairwise(p.index):
            assert t2 in w.index
            if t2 not in weights.index:
                w.ix[t2] = self.__forward(w.ix[t1], p.ix[t1], p.ix[t2])

        self.__prices = p
        self.__weights = w

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

    #@property
    #def trades_relative(self):
    #    """
    #    :return: trades as fraction of a portfolio per asset and day
    #    """
    #    trade_in_usd = self.position.ffill().diff().fillna(0.0)*self.prices.ffill()
    #    n = self.nav.series
    #    return pd.DataFrame({key: trade_in_usd[key]/n for key in trade_in_usd.keys()})

    def subportfolio(self, assets):
        return Portfolio(prices=self.prices[assets], weights=self.weights[assets])

    #def nav_adjusted(self, size=1e6, flatfee=0.0, basispoints=20, threshold=0.01):
    #    r0 = self.nav.returns
    #    r1 = self.trades_relative.abs().sum(axis=1)*basispoints / 10000
    #    r2 = self.trade_count(threshold).sum(axis=1)*flatfee / (self.nav.series * size)

    #    # make this a nav again
    #    r = r0 - r1 - r2
    #    return Nav((1 + r).cumprod())

    #def trade_count(self, threshold=0.01):
    #    t = self.trades_relative.abs()
    #    t[t >= threshold] = 1
    #    t[t < threshold] = 0
    #    return t

    def __mul__(self, other):
        return Portfolio(self.prices, other * self.weights)

    def __rmul__(self, other):
        return self.__mul__(other)

    def subsample(self, t):
        return Portfolio(prices=self.prices, weights=self.weights.ix[t])

    def apply(self, function, axis=0):
        return Portfolio(prices=self.prices, weights=self.weights.apply(function, axis=axis))

    def plot(self, colors=None):
        import matplotlib.pyplot as plt
        colors = colors or [a['color'] for a in plt.rcParams['axes.prop_cycle']]
        ax1 = plt.subplot(211)
        (100 * (self.nav.series)).plot(ax=ax1, color=colors[0])
        plt.legend(["NAV"], loc=2)

        ax2 = plt.subplot(413, sharex=ax1)
        (100 * (self.leverage)).plot(ax=ax2, color=colors[1])
        ax2.set_ylim([-10, 110])
        plt.legend(["Leverage"], loc=2)

        ax3 = plt.subplot(414, sharex=ax1)
        (100 * (self.nav.drawdown)).plot(ax=ax3, color=colors[2])
        plt.legend(["Drawdown"], loc=2)

        plt.tight_layout()

        return [ax1, ax2, ax3]

    @property
    def trading_days(self):
        __fundsize = 1e6
        days = (__fundsize*self.position).diff().abs().sum(axis=1)
        return days[days > 1].index