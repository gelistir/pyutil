import logging

import numpy as np
import pandas as pd

from pyutil.nav.nav import Nav
from pyutil.performance.periods import period_returns, periods
from pyutil.portfolio.maths import xround, buy_or_sell
from pyutil.timeseries.timeseries import ytd, mtd


#def merge(portfolios, axis=0):
#    prices = pd.concat([p.prices for p in portfolios], axis=axis, verify_integrity=True)
#    weights = pd.concat([p.weights for p in portfolios], axis=axis, verify_integrity=True)
#    return Portfolio(prices, weights.fillna(0.0))


def forward(w1, w2, p1, p2):
    # w1 weight at time t1
    # w2 weight at time t2
    # p1 price at time t1
    # p2 price at time t2
    # return the extrapolated weights at time t2

    # in some scenarios the weight are set even before a first price is known.

    valid = np.isfinite(w2)

    if np.all(valid):
        # all weights are valid
        return w2
    elif np.all(~valid):
        # not a single weight is valid
        cash = 1.0 - np.nansum(w1)
        value = w1 * (p2 / p1)
        w = value / (np.nansum(value) + cash)
        w[np.isnan(w)] = 0.0
        return w
    else:
        assert False, "Partial definition of weights. Problem! w2: {0}, w1: {1}".format(w2, w1)


class Portfolio(object):
    def iron_threshold(self, threshold=0.02):
        """
        Iron a portfolio, do not touch the last index

        :param threshold:
        :return:
        """

        # make sure the order is correct...
        self.__logger.debug("Head of weights\n{0}".format(self.weights.head(4)))
        self.__logger.debug("Head of prices\n{0}".format(self.prices.head(4)))
        w = self.weights[self.assets].values
        p = self.prices[self.assets].values

        assert w.shape == p.shape

        for i in range(1, p.shape[0] - 1):
            if np.abs(w[i] - w[i-1]).max() <= threshold:
                w[i] = forward(w1=w[i-1], w2=np.nan*w[i-1], p1=p[i-1], p2=p[i])

        p = pd.DataFrame(index=self.prices.index, columns=self.assets, data=p)
        w = pd.DataFrame(index=self.weights.index, columns=self.assets, data=w)

        return Portfolio(prices=p, weights=w)

    def iron_time(self, rule):
        # make sure the order is correct...
        w = self.weights[self.assets].values
        p = self.prices[self.assets].values

        assert w.shape == p.shape

        # take the weights of the underlying portfolio
        index = self.weights.index

        moments = [0]

        for t in self.weights.ix[:-1].resample(rule=rule).last().index:
            moments.append(len(index[index <= t]) - 1)

        for i in range(1, p.shape[0] - 1):
            if i not in moments:
                w[i] = forward(w1=w[i - 1], w2=np.nan*w[i-1], p1=p[i - 1], p2=p[i])

        p = pd.DataFrame(index=self.prices.index, columns=self.assets, data=p)
        w = pd.DataFrame(index=self.weights.index, columns=self.assets, data=w)

        return Portfolio(prices=p, weights=w)

    def __init__(self, prices, weights, logger=None):
        self.__logger = logger or logging.getLogger("LWM")

        assert set(weights.keys()) <= set(prices.keys()), "Key for weights not subset of keys for prices"
        assert prices.index.equals(weights.index), "Index for prices and weights have to match"

        self.__logger.info("prices.index === weights.index")
        self.__prices = prices[weights.keys()].ffill()

        p = self.__prices.values
        w = weights.values

        for i in range(1, p.shape[0]):
            w[i] = forward(w1=w[i - 1], w2=w[i], p1=p[i - 1], p2=p[i])

        self.__weights = pd.DataFrame(index=prices.index, columns=prices.keys(), data=w).fillna(0.0)

    def __repr__(self):
        return "Portfolio with assets: {0}".format(list(self.__weights.keys()))

    @property
    def cash(self):
        return 1.0 - self.weights.ffill().sum(axis=1)

    @property
    def assets(self):
        return sorted(list(self.__prices.keys()))

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
            d[asset] = ww * rr

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
        frame = self.weights.ffill().groupby(by=symbolmap, axis=1).sum()
        frame["total"] = frame.sum(axis=1)
        return frame

    def snapshot(self, n=5):
        today = self.index[-1]
        offsets = periods(today)

        a = self.weighted_returns.apply(period_returns, offset=offsets).transpose()[
            ["Month-to-Date", "Year-to-Date"]]
        t = self.trading_days[-n:]

        b = self.weights.ffill().ix[t].rename(index=lambda x: x.strftime("%d-%b-%y")).transpose()
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

    def subportfolio(self, assets):
        return Portfolio(prices=self.prices[assets], weights=self.weights[assets])

    def __mul__(self, other):
        return Portfolio(self.prices, other * self.weights)

    def __rmul__(self, other):
        return self.__mul__(other)

    def apply(self, function, axis=0):
        return Portfolio(prices=self.prices, weights=self.weights.apply(function, axis=axis))

    def plot(self, colors=None, tradingDays=False):
        import matplotlib.pyplot as plt

        colors = colors or [a['color'] for a in plt.rcParams['axes.prop_cycle']]
        ax1 = plt.subplot(211)
        (100 * (self.nav.series)).plot(ax=ax1, color=colors[0])
        if tradingDays:
            x1, x2, y1, y2 = plt.axis()
            plt.vlines(x=self.trading_days, ymin=y1, ymax=y2, colors="red")
        plt.grid()
        plt.legend(["NAV"], loc=2)

        ax2 = plt.subplot(614, sharex=ax1)
        (100 * (self.leverage)).plot(ax=ax2, color=colors[1])
        ax2.set_ylim([-10, 110])
        plt.legend(["Leverage"], loc=2)
        plt.grid()
        # ax2.set_xticklabels(())

        ax3 = plt.subplot(615, sharex=ax1)
        (100 * (self.nav.drawdown)).plot(ax=ax3, color=colors[2])
        plt.legend(["Drawdown"], loc=2)
        plt.grid()
        # ax3.set_xticklabels(())

        ax4 = plt.subplot(616, sharex=ax1)
        plt.grid()
        (100 * (self.weights.max(axis=1))).plot(ax=ax4, color=colors[3])
        plt.legend(["Max Weight"], loc=2)
        plt.grid()

        return [ax1, ax2, ax3, ax4]

    @property
    def trading_days(self):
        __fundsize = 1e6
        days = (__fundsize * self.position).diff().abs().sum(axis=1)
        return sorted(list(days[days > 1].index))

    def ffill(self, prices=False):
        if prices:
            return Portfolio(prices=self.prices.ffill(), weights=self.weights.ffill())
        else:
            return Portfolio(prices=self.prices, weights=self.weights.ffill())

    def transaction_report(self, capital=1e7, n=2):
        d = dict()
        nav = self.nav.series
        prices = self.prices.ffill()

        old_position = pd.Series({asset: 0.0 for asset in self.assets})

        for trading_day in self.trading_days:
            nav_today = nav.ix[trading_day]
            p = prices.ix[trading_day]

            # new goal position
            pos = self.weights.ix[trading_day] * capital * nav_today / p
            pos = pos.apply(xround, (n,))

            # compute the trade to get to position
            trade = pos - old_position
            trade = trade[trade.abs() > 0]

            # the new position
            old_position = pos

            units = trade
            amounts = units * p.ix[trade.index]
            d[trading_day] = pd.DataFrame({"Amount": amounts, "Units": units})

        p = pd.concat(d)
        p["Type"] = p["Amount"].apply(buy_or_sell)
        return p

    def to_json(self):
        """
        Convert portfolio into a big dictionary (e.g.
        :return:
        """

        def __f(ts):
            return {"{0}".format(t.strftime("%Y%m%d")): v for t, v in ts.dropna().iteritems()}

        def __g(frame):
            return {key: __f(series) for key, series in frame.iteritems()}

        return {"weight": __g(self.weights), "price": __g(self.prices), "returns": __f(self.nav.returns)}

    def ytd(self, today=None):
        return Portfolio(prices=ytd(self.prices, today=today), weights=ytd(self.weights, today=today))

    def mtd(self, today=None):
        return Portfolio(prices=mtd(self.prices, today=today), weights=mtd(self.weights, today=today))


if __name__ == '__main__':
    weights = pd.DataFrame(columns=["A", "B"], index=[1, 2], data=[[0.5, 0.5], [np.NaN, np.NaN]])
    print(weights)

    prices = pd.DataFrame(columns=["A", "B"], index=[1, 2], data=100)
    print(prices)

    p = Portfolio(prices=prices, weights=weights)
