import pandas as pd
import numpy as np

from pyutil.portfolio.portfolio import Portfolio


class PortfolioBuilder(object):
    def __init__(self, prices):
        self.__prices = prices
        self.__weights = pd.DataFrame(index = prices.index, columns=prices.keys(), data=np.nan)

    @property
    def returns(self):
        return self.__prices.pct_change()

    @property
    def weights(self):
        return self.__weights

    @property
    def prices(self):
        return self.__prices

    def build(self, logger=None):
        return Portfolio(prices = self.__prices, weights = self.__weights, logger=logger)

    def forward(self, t):
        # We move weights to t
        p = self.__prices.truncate(after=t)
        p2 = p.ix[p.index[-1]]
        p1 = p.ix[p.index[-2]]
        w1 = self.weights.ix[p.index[-2]]
        cash = 1.0 - np.nansum(w1)
        value = w1 * (p2 / p1)
        w = value / (np.nansum(value) + cash)
        w[np.isnan(w)] = 0.0
        return w

if __name__ == '__main__':
    prices = pd.DataFrame(columns=["A", "B"], index=[1, 2], data=100)
    print(prices)

    builder = PortfolioBuilder(prices=prices)
    builder.weights.ix[1] = [0.2, 0.8]
    #builder.weights.ix[2] = pd.Series({"A": 0.3, "B": 0.7})
    builder.weights.ix[2] = builder.forward(t=2)
    print(builder.weights)
    #assert False

    portfolio = builder.build()
    print(portfolio.weights)
    print(sorted(builder.prices.columns.tolist()))

