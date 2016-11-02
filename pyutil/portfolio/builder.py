import pandas as pd
import numpy as np

from pyutil.portfolio.portfolio import Portfolio


class PortfolioBuilder(object):
    def __init__(self, prices):
        self.__prices = prices.ffill()
        self.__weights = pd.DataFrame(index = prices.index, columns=prices.keys(), data=np.nan)
        self.__returns = self.__prices.pct_change()

    @property
    def timestamps(self):
        return self.__prices.index

    @property
    def assets(self):
        return self.__prices.keys()

    @property
    def returns(self):
        return self.__returns

    @property
    def weights(self):
        return self.__weights

    def current_weights(self, t):
        return self.__weights.ix[t].dropna()

    def current_prices(self, t):
        return self.__prices.ix[t].dropna()

    @property
    def cash(self):
        return self.weights.sum(axis=1)

    @property
    def prices(self):
        return self.__prices

    def build(self, logger=None):
        return Portfolio(prices = self.__prices, weights = self.__weights, logger=logger)

    def __before(self, t):
        tt = self.timestamps.get_loc(t)
        return self.timestamps[tt-1]

    def forward(self, t):
        # We move weights to t
        yesterday = self.__before(t)

        w1 = self.current_weights(yesterday)
        r = self.returns.ix[t]

        # fraction of the cash in the portfolio yesterday
        cash = 1.0 - w1.sum()
        # new value of each position
        value = w1 * (r + 1)

        self.weights.ix[t] = value / (value.sum() + cash)
