import pandas as pd
from pyutil.performance.periods import period_returns, periods
from pyutil.nav.nav import Nav
from pyutil.portfolio.maths import xround, buy_or_sell
from pyutil.timeseries.timeseries import subsample
from itertools import tee


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

        # in some scenarios the weight are set even before a first price is known.
        w1.ix[p1.isnull()] = np.nan
        w1 = w1.dropna()

        cash = 1 - w1.sum()                   # cash at time t1
        pos = w1 / p1                         # pos at time t1

        value = pos * p2                      # value of asset at time t2
        return value / (value.sum() + cash)

    def iron_threshold(self, threshold=0.02):
        """
        Iron a portfolio, do not touch the last index

        :param threshold:
        :return:
        """
        w = self.weights.ffill(inplace=False)
        p = self.prices.ffill(inplace=False)

        for t1, t2 in self.__pairwise(w.ix[:-1].index):
            if (w.ix[t2] - w.ix[t1]).abs().max() <= threshold:
                # no trading hence we update the weights forward
                w.ix[t2] = self.__forward(w1=w.ix[t1], p1=p.ix[t1], p2=p.ix[t2])

        return Portfolio(prices=p, weights=w)

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
        for asset in weights.keys():
            assert asset in prices.keys()

        # make sure the weights are a subset of the prices
        if prices.index.equals(weights.index):
            self.__prices = prices.ffill()
            self.__weights = weights.ffill().fillna(0.0)
        else:
            for time in weights.index:
                assert time in prices.index

            # only take into account prices after the first weight!
            p = prices.ffill()

            # set the weights
            w = pd.DataFrame(index=prices.index, columns=prices.keys(), data=0.0)
            w.ix[weights.index] = weights

            # loop over all times
            for t1, t2 in self.__pairwise(prices.index):
                if t2 not in weights.index:
                    w.ix[t2] = self.__forward(w.ix[t1], p.ix[t1], p.ix[t2])

            self.__prices = prices.ffill()
            self.__weights = w.ffill().fillna(0.0)

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

    def snapshot2(self, n=5):
        today = self.index[-1]
        offsets = periods(today)

        a = 100*self.weighted_returns.apply(period_returns, offset=offsets).transpose()[["Month-to-Date", "Year-to-Date"]]
        tt = self.trading_days[-n:]

        b = 100*self.weights.ffill().ix[tt].rename(index=lambda x: x.strftime("%d-%b-%y")).transpose()
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

    #def nav_adjusted(self, size=1e6, flatfee=0.0, basispoints=20, threshold=0.01):
    #    r0 = self.nav.returns
    #    r1 = self.trades_relative.abs().sum(axis=1)*basispoints / 10000
    #    r2 = self.trade_count(threshold).sum(axis=1)*flatfee / (self.nav.series * size)

    #    # make this a nav again
    #    r = r0 - r1 - r2
    #    return Nav((1 + r).cumprod())

    def __mul__(self, other):
        return Portfolio(self.prices, other * self.weights)

    def __rmul__(self, other):
        return self.__mul__(other)

    # def subsample(self, t):
    #     return Portfolio(prices=self.prices, weights=self.weights.ix[t])

    def apply(self, function, axis=0):
        return Portfolio(prices=self.prices, weights=self.weights.apply(function, axis=axis))

    def plot(self, colors=None, tradingDays=None):
        import matplotlib.pyplot as plt
        colors = colors or [a['color'] for a in plt.rcParams['axes.prop_cycle']]
        ax1 = plt.subplot(211)
        (100 * (self.nav.series)).plot(ax=ax1, color=colors[0])
        if tradingDays:
            x1, x2, y1, y2 = plt.axis()
            plt.vlines(x=self.trading_days, ymin=y1, ymax=y2, colors="red")

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
            pos = self.weights.ix[trading_day]*capital*nav_today / p
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


