import pandas as pd
import numpy as np

from .portfolio import Portfolio


class PortfolioBuilder(object):
    def __init__(self, prices):
        self.__prices = prices.ffill()
        self.__weights = pd.DataFrame(index = prices.index, columns=prices.keys(), data=np.nan)
        self.__returns = self.__prices.pct_change()

        self.__before = {prices.index[i]: prices.index[i-1] for i in range(1, len(prices.index))}

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

    @property
    def cash(self):
        return 1-self.weights.sum(axis=1)

    @property
    def prices(self):
        return self.__prices

    def build(self, logger=None):
        return Portfolio(prices = self.__prices, weights = self.__weights, logger=logger)

    def forward(self, t):
        # We move weights to t
        yesterday = self.__before[t]

        w1 = self.__weights.ix[yesterday].dropna()

        # fraction of the cash in the portfolio yesterday
        cash = self.cash[yesterday]

        # new value of each position
        value = w1 * (self.returns.ix[t] + 1)

        self.weights.ix[t] = value / (value.sum() + cash)
