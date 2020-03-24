import pandas as pd
import numpy as np

from pyutil.performance.return_series import drawdown
from pyutil.timeseries.merge import merge as merge_in_t

_factor = 1e6

def merge(portfolios, axis=0):
    prices = pd.concat([p.prices for p in portfolios], axis=axis, verify_integrity=True)
    weights = pd.concat([p.weights for p in portfolios], axis=axis, verify_integrity=True)
    return Portfolio(prices, weights.fillna(0.0))


def similar(a, b, eps=1e-6):
    """
    Useful for unit-testing
    :param a: portfolio a
    :param b: portfolio b
    :param eps: maximal difference in weights and prices tolerated
    :return: True (if index, assets, prices and weights are similar) otherwise False
    """
    if not (isinstance(a, Portfolio) and isinstance(b, Portfolio)):
        return False

    if not (list(a.index) == list(b.index) and list(a.assets) == list(b.assets)):
        return False

    delta_w = (a.weights - b.weights).abs().max().max()
    delta_p = (a.prices - b.prices).abs().max().max()

    if not (delta_w < eps and delta_p < eps):
        return False

    return True
#
#
# def one_over_n(prices):
#     """
#     Args:
#         prices: Frame of prices
#
#     Returns: The classic 1/n portfolio
#     """
#     def __f(x):
#         n = x.notnull().sum()
#         y = pd.Series(index=x.index)
#         if n > 0:
#             y[x.notnull()] = 1 / n
#         return y
#
#     return Portfolio(prices=prices, weights=prices.ffill().apply(__f, axis=1))


class Portfolio(object):
    def rename(self, names):
        """ names is a dictionary """
        self.weights.rename(columns=names, inplace=True)
        self.prices.rename(columns=names, inplace=True)
        return self

    @staticmethod
    def __series2frame(index, series):
        f = pd.DataFrame(index=index, columns=series.index)
        for key in series.index:
            f[key] = series[key]

        return f

    @staticmethod
    def merge(new, old=None):
        assert isinstance(new, Portfolio)
        if old is not None:
            assert isinstance(old, Portfolio)
            w = merge_in_t(new=new.weights, old=old.weights)
            p = merge_in_t(new=new.prices, old=old.prices)
            return Portfolio(prices=p, weights=w)
        else:
            return new

    @staticmethod
    def fromPosition(prices, position=None, cash=None):
        cash = cash or 0

        if position is None:
            position = pd.DataFrame(index=prices.index, columns=prices.keys(), data=0.0)
            cash = 1

        if isinstance(position, pd.Series):
            position = Portfolio.__series2frame(index=prices.index, series=position)

        value = position * prices
        total = value.sum(axis=1) + cash

        return Portfolio(prices=prices, weights=value.apply(lambda x: x/total))

    def copy(self):
        return Portfolio(prices=self.prices.copy(), weights=self.weights.copy())

    def iron_threshold(self, threshold=0.02):
        """
        Iron a portfolio, do not touch the last index

        :param threshold:
        :return:
        """
        x = self.copy()

        for t, portfolio in x.loop(self.index[:-1]):
            # You notice some difference...
            diff = (portfolio.weights.loc[t] - self.__weights.loc[t]).abs().max()
            if diff > threshold:
                portfolio[t] = self.__weights.loc[t]

        return x

    def iron_time(self, rule):
        # make sure the order is correct...
        x = self.copy()

        moments = [self.index[0]]

        # we need timestamps from the underlying series not the end of the intervals!
        resample = self.weights.resample(rule=rule).last()
        for t in resample.index:
            moments.append([a for a in self.index if a <= t][-1])

        # run through all dates expect the last one...
        for t, portfolio in x.loop(self.index[:-1]):
            if t in moments:
                portfolio[t] = self.__weights.loc[t]

        return x

    def __init__(self, prices, weights=None):
        assert not prices.index.has_duplicates, "Price Index has duplicates"
        assert prices.index.is_monotonic_increasing, "Price Index is not increasing"

        # forward interpolate the prices
        self.__prices = prices.ffill()

        # make it a frame
        if isinstance(weights, pd.Series):
            weights = self.__series2frame(index = self.__prices.index, series=weights)

        if weights is not None:
            assert weights.index.equals(prices.index)
            assert set(weights.keys()) <= set(prices.keys())
            self.weights = weights
        else:
            self.weights = pd.DataFrame(index=self.prices.index, columns=self.prices.keys(), data=np.nan)

    def __repr__(self):
        return "Portfolio with assets: {0}".format(list(self.__weights.keys()))

    @property
    def cash(self):
        return 1.0 - self.weights.sum(axis=1)

    @property
    def position(self):
        return (self.weights / self.prices).apply(lambda x: x*self.nav)

    @property
    def assets(self) -> list:
        return list(self.__prices.sort_index(axis=1).columns)

    @property
    def prices(self) -> pd.DataFrame:
        return self.__prices

    @property
    def weights(self) -> pd.DataFrame:
        return self.__weights

    @weights.setter
    def weights(self, value):
        self.__weights = value
        assert (self.cash >= 0).all(), "Negative cash"

    @property
    def asset_returns(self):
        return self.prices.pct_change()

    @property
    def nav(self) -> pd.Series:
        return (self.returns + 1.0).cumprod()

    @property
    def returns(self) -> pd.Series:
        return self.weighted_returns.sum(axis=1)

    @property
    def weighted_returns(self):
        r = self.asset_returns.fillna(0.0)
        return pd.DataFrame({a: r[a]*self.weights[a].dropna().shift(1).fillna(0.0) for a in self.assets})

    @property
    def index(self):
        return self.prices.index

    @property
    def leverage(self):
        return self.weights.abs().sum(axis=1).dropna().apply(float)

    def truncate(self, before=None, after=None):
        return Portfolio(prices=self.prices.truncate(before=before, after=after),
                         weights=self.weights.truncate(before=before, after=after))

    @property
    def empty(self):
        return len(self.index) == 0

    @property
    def weight_current(self):
        w = self.weights.ffill()
        a = w.loc[w.index[-1]]
        a.index.name = "weight"
        return a

    def sector(self, symbolmap, total=False):
        frame = self.weights.ffill().groupby(by=symbolmap, axis=1).sum()
        if total:
            frame["Total"] = frame.sum(axis=1)
        return frame

    def tail(self, n=10):
        w = self.weights.tail(n)
        return Portfolio(prices=self.prices.loc[w.index], weights=w)

    def head(self, n=10):
        w = self.weights.head(n)
        return Portfolio(prices=self.prices.loc[w.index], weights=w)

    def subportfolio(self, assets):
        return Portfolio(prices=self.prices[assets], weights=self.weights[assets])

    def __mul__(self, other):
        return Portfolio(prices=self.prices, weights=other * self.weights)

    def __rmul__(self, other):
        return self.__mul__(other)

    def apply(self, function, axis=0):
        return Portfolio(prices=self.prices, weights=self.weights.apply(function, axis=axis))

    # @property
    # def trading_days(self):
    #     __fundsize = 1e6
    #     days = (__fundsize * self.position).diff().abs().sum(axis=1)
    #     return sorted(list(days[days > 1].index))
    #
    # def state(self, symbols=None):
    #     # get the last 5 trading days
    #     trade_events = self.trading_days[-5:-1]
    #     today = self.index[-1]
    #     if today not in trade_events:
    #         trade_events.append(today)
    #
    #     #a = self.weighted_returns.apply(lambda x: )
    #     offsets = periods(today=self.index[-1])
    #
    #     a = self.weighted_returns.apply(period_returns, offset=offsets).transpose()[
    #         ["Month-to-Date", "Year-to-Date"]]
    #
    #     # extract the weights at all those trade events
    #     weights = self.weights.ffill().loc[trade_events].transpose()
    #
    #     # that's the portfolio where today has been forwarded to (from yesterday),
    #     p = Portfolio(prices=self.prices, weights=self.weights.copy()).forward(today)
    #
    #     weights = weights.rename(columns=lambda x: x.strftime("%d-%b-%y"))
    #
    #     weights["Extrapolated"] = p.weights.loc[today]
    #     weights["Gap"] = self.weights.loc[today] - p.weights.loc[today]
    #     weights.index.name = "Symbol"
    #     frame = pd.concat((a, weights), axis=1)
    #
    #     if symbols is not None:
    #         all = {symbol.name: symbol for symbol in symbols}
    #         frame["group"] = pd.Series({s : all[s].group.name for s in frame.index})
    #         frame["internal"] = pd.Series({s : all[s].internal for s in frame.index})
    #
    #
    #         sector_weights = frame.groupby(by="group")["Extrapolated"].sum()
    #         frame["Sector Weight"] = frame["group"].apply(lambda x: sector_weights[x])
    #         frame["Relative Sector"] = frame["Extrapolated"] / frame["Sector Weight"]
    #
    #     frame.index.name = "Symbol"
    #     return frame
    #
    # @property
    # def last_dates(self):
    #     return self.prices.apply(lambda x: x.last_valid_index()).sort_values(ascending=True)

    def to_frame(self, name=""):
        frame = self.nav.to_frame("{n}-nav".format(n=name))
        frame["{n}-drawdown".format(n=name)] = drawdown(series=self.returns)
        frame["{n}-leverage".format(n=name)] = self.leverage
        frame["{n}-cash".format(n=name)] = self.cash
        return frame

    # def to_csv(self, folder, name=None):
    #     if not os.path.exists(folder):
    #         os.makedirs(folder)
    #     name = name or ""
    #     self.weights.to_csv(os.path.join(folder, "{n}_weights.csv".format(n=name)))
    #     self.prices.to_csv(os.path.join(folder, "{n}_prices.csv".format(n=name)))
    #
    # @staticmethod
    # def read_csv(folder, name=None):
    #     name = name or ""
    #     w = pd.read_csv(os.path.join(folder, "{n}_weights.csv".format(n=name)), index_col=0, parse_dates=True)
    #     p = pd.read_csv(os.path.join(folder, "{n}_prices.csv".format(n=name)), index_col=0, parse_dates=True)
    #     return Portfolio(prices=p, weights=w)

    def __setitem__(self, t, weights):
        assert set(weights.index) == set(self.assets)
        assert t in self.index, "Unknown timestamp"
        self.weights.loc[t] = weights

    def loop(self, index=None):
        # start with the first index and the first portfolio
        i = self.index

        if index is not None:
            i = index

        assert len(i) >= 2

        # yield the first index and the portfolio
        yield i[0], self

        # pairwise iteration over (first, second), (second, third), ... index
        for t1, t2 in zip(i[:-1], i[1:]):
            # now, the weights are not invariant. The market has moved between t1 and t2
            # Hence we need  to update the weights correctly
            # What hasn't changed is the position though.

            # old position

            v = (self.asset_returns.loc[t2] + 1.0) * self.weights.loc[t1]
            self[t2] = v / (v.sum() + self.cash[t1])

            yield t2, self
