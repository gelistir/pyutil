import numpy as np
import pandas as pd

from ..performance.summary import fromReturns
from .maths import xround, buy_or_sell
from ..performance.periods import period_returns, periods
from ..timeseries.timeseries import ytd, mtd


def merge(portfolios, axis=0):
    prices = pd.concat([p.prices for p in portfolios], axis=axis, verify_integrity=True)
    weights = pd.concat([p.weights for p in portfolios], axis=axis, verify_integrity=True)
    return Portfolio(prices, weights.fillna(0.0))

def read_csv(file):
    x = pd.read_csv(file, header=[0,1], index_col=0, parse_dates=True)
    return Portfolio(x["price"], x["weight"])


class Portfolio(object):
    def copy(self):
        return Portfolio(prices=self.prices.copy(), weights=self.weights.copy())

    def iron_threshold(self, threshold=0.02):
        """
        Iron a portfolio, do not touch the last index

        :param threshold:
        :return:
        """
        portfolio = self.copy()
        for yesterday, today in zip(self.index[:-2], self.index[1:-1]):
            if (portfolio.weights.loc[today] - portfolio.weights.loc[yesterday]).abs().max() <= threshold:
                portfolio.forward(today, yesterday=yesterday)

        return portfolio

    def iron_time(self, rule):
        # make sure the order is correct...
        portfolio = self.copy()

        moments = [self.index[0]]

        # we need timestamps from the underlying series not the end of intervals!
        resample = self.weights.resample(rule=rule).last()
        for t in resample.index:
            moments.append(self.weights[self.weights.index <= t].index[-1])

        for date in self.weights.index[:-1]:
            if date not in moments:
                portfolio.forward(date)

        return portfolio

    def forward(self, t, yesterday=None):
        # We move weights to t
        yesterday = yesterday or self.__before[t]

        w1 = self.__weights.loc[yesterday].dropna()

        # fraction of the cash in the portfolio yesterday
        cash = 1 - w1.sum()

        # new value of each position
        value = w1 * (self.asset_returns.loc[t].fillna(0.0) + 1)

        self.weights.loc[t] = value / (value.sum() + cash)

        return self

    def __init__(self, prices, weights=None): #, **kwargs):
        # if you don't specify any weights, we initialize them with nan
        if weights is None:
            weights = pd.DataFrame(index=prices.index, columns=prices.keys(), data=np.nan)

        # If weights is a Series, each weight per asset!
        if isinstance(weights, pd.Series):
            w = pd.DataFrame(index=prices.index, columns=weights.keys())
            for t in w.index:
                w.loc[t] = weights

            weights = w

        assert set(weights.keys()) <= set(prices.keys()), "Key for weights not subset of keys for prices"
        assert prices.index.equals(weights.index), "Index for prices and weights have to match"

        assert not prices.index.has_duplicates, "Price Index has duplicates"
        assert not weights.index.has_duplicates, "Weights Index has duplicates"

        assert prices.index.is_monotonic_increasing, "Price Index is not increasing"
        assert weights.index.is_monotonic_increasing, "Weight Index is not increasing"

        self.__prices = prices.ffill()

        for key, w in weights.items():
            # check the weight series...
            series = w.copy()
            series.index = range(0, len(series))
            # remove all nans
            series = series.dropna()

            # compute the length of the maximal gap
            max_gap = pd.Series(data=series.index).diff().dropna().max()
            assert not max_gap > 1, "There are gaps in the series {0} and gap {1}".format(w, max_gap)

        self.__weights = weights

        self.__before = {today : yesterday for today, yesterday in zip(prices.index[1:], prices.index[:-1])}
        self.__r = self.__prices.pct_change()

        #self.__dict = copy.deepcopy(kwargs)

    #@property
    #def meta(self):
    #    return self.__dict

    def __repr__(self):
        return "Portfolio with assets: {0}".format(list(self.__weights.keys()))

    @property
    def cash(self):
        return 1.0 - self.leverage

    @property
    def assets(self):
        return list(self.__prices.sort_index(axis=1).columns)

    @property
    def prices(self):
        return self.__prices

    @property
    def weights(self):
        return self.__weights

    @property
    def asset_returns(self):
        return self.__r

    @property
    def nav(self):
        return fromReturns(self.weighted_returns.sum(axis=1))

    @property
    def weighted_returns(self):
        r = self.asset_returns.fillna(0.0)
        return pd.DataFrame({a: r[a]*self.weights[a].dropna().shift(1).fillna(0.0) for a in self.assets})

    @property
    def index(self):
        return self.prices.index

    @property
    def leverage(self):
        return self.weights.sum(axis=1).dropna()

    def summary(self, t0=None, t1=None, alpha=0.95, periods=None, r_f=0):
        x = self.nav.truncate(before=t0, after=t1).summary(alpha=alpha, periods=periods, r_f=r_f)
        y = self.leverage.truncate(before=t0, after=t1).summary()
        return pd.concat((x,y), axis=0)

    def truncate(self, before=None, after=None):
        return Portfolio(prices=self.prices.truncate(before=before, after=after),
                    weights=self.weights.truncate(before=before, after=after))


    @property
    def empty(self):
        """
        Return True if the portfolio is empty, False otherwise. A portfolio is empty if any only if both prices and weights are empty

        :return:
        """
        return len(self.index) == 0

    @property
    def weight_current(self):
        w = self.weights.ffill()
        a = w.loc[w.index[-1]]
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

        b = self.weights.ffill().loc[t].rename(index=lambda x: x.strftime("%d-%b-%y")).transpose()
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
        return pd.concat(d, axis=0, names=["category","rank"])

    def tail(self, n=10):
        w = self.weights.tail(n)
        p = self.prices.loc[w.index]
        return Portfolio(p, w)

    @property
    def position(self):
        return pd.DataFrame({k: self.weights[k] * self.nav / self.prices[k] for k in self.assets})

    def subportfolio(self, assets):
        return Portfolio(prices=self.prices[assets], weights=self.weights[assets])

    def __mul__(self, other):
        return Portfolio(self.prices, other * self.weights)

    def __rmul__(self, other):
        return self.__mul__(other)

    def apply(self, function, axis=0):
        return Portfolio(prices=self.prices, weights=self.weights.apply(function, axis=axis))

    @property
    def trading_days(self):
        __fundsize = 1e6
        days = (__fundsize * self.position).diff().abs().sum(axis=1)
        return sorted(list(days[days > 1].index))

    def transaction_report(self, capital=1e7, n=2):
        d = dict()
        nav = self.nav
        prices = self.prices.ffill()

        old_position = pd.Series({asset: 0.0 for asset in self.assets})

        for trading_day in self.trading_days:
            nav_today = nav.loc[trading_day]
            p = prices.loc[trading_day]

            # new goal position
            pos = self.weights.loc[trading_day] * capital * nav_today / p
            pos = pos.apply(xround, (n,))

            # compute the trade to get to position
            trade = pos - old_position
            trade = trade[trade.abs() > 0]

            # the new position
            old_position = pos

            units = trade
            amounts = units * p.loc[trade.index]
            d[trading_day] = pd.DataFrame({"Amount": amounts, "Units": units})

        p = pd.concat(d)
        p["Type"] = p["Amount"].apply(buy_or_sell)
        return p

    def ytd(self, today=None):
        return Portfolio(prices=ytd(self.prices, today=today), weights=ytd(self.weights, today=today))

    def mtd(self, today=None):
        return Portfolio(prices=mtd(self.prices, today=today), weights=mtd(self.weights, today=today))

    @property
    def state(self):
        # get the last 5 trading days
        trade_events = self.trading_days[-5:-1]
        today = self.index[-1]
        if today not in trade_events:
            trade_events.append(today)

        # extract the weights at all those trade events
        weights = self.weights.ffill().loc[trade_events].transpose()

        # that's the portfolio where today has been forwarded to (from yesterday),
        p = Portfolio(prices=self.prices, weights=self.weights.copy()).forward(today)

        weights = 100.0 * weights.rename(columns=lambda x: x.strftime("%d-%b-%y"))
        weights["Extrapolated"] = 100.0 * p.weights.loc[today]
        weights["Gap"] = 100.0 * (self.weights.loc[today] - p.weights.loc[today])
        return weights

    def plot(self, tradingDays=False, figsize=(16,10)):

        import matplotlib.pyplot as plt

        f = plt.figure(figsize=figsize)

        ax1 = f.add_subplot(211)
        (100 * self.nav).plot(ax=ax1, color="blue")
        if tradingDays:
            x1, x2, y1, y2 = plt.axis()
            plt.vlines(x=self.trading_days, ymin=y1, ymax=y2, colors="red")
        plt.legend(["NAV"], loc=2)

        ax2 = f.add_subplot(413,sharex=ax1)
        (100*self.nav.drawdown).plot.area(ax=ax2, alpha=0.2, color="red", linewidth=2)
        plt.legend(["Drawdown"], loc=2)

        ax3 = plt.subplot(414, sharex=ax1)
        (100 * self.leverage).plot(ax=ax3, color='green')
        ax3.set_ylim([-10, 110])

        (100 * self.weights.max(axis=1)).plot(ax=ax3, color='blue')
        plt.legend(["Leverage","Max Weight"], loc=2)

        f.subplots_adjust(hspace=0.05)
        plt.setp([a.get_xticklabels() for a in f.axes[:-1]], visible=False)

        return f

    def to_csv(self, file):
        pd.concat({"price": self.prices, "weight": self.weights}, axis=1).to_csv(file)


    def __eq__(self, other):
        if type(other) is type(self):
            return self.prices.equals(other.prices) and self.weights.equals(other.weights)
        return False

    def __ne__(self, other):
        return not self.__eq__(other)

    @property
    def trade_usd(self):
        # the amount of USD traded per asset on trading days
        return self.trade_abs * self.prices.loc[self.trading_days]

    @property
    def trade_rel(self):
        # the fraction of capital traded on trading days
        return self.trade_usd.div(self.nav.loc[self.trading_days], axis=0)

    @property
    def trade_abs(self):
        # the number of shares (etc.) traded on trading days assuming a fundsize of 1
        return (self.position.fillna(0.0).diff()).loc[self.trading_days]