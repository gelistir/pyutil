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

    @property
    def assets(self):
        return sorted(self.__prices.columns.tolist())

    def build(self, logger=None):
        return Portfolio(prices = self.__prices, weights = self.__weights, logger=logger)


if __name__ == '__main__':
    prices = pd.DataFrame(columns=["A", "B"], index=[1, 2], data=100)
    print(prices)

    builder = PortfolioBuilder(prices=prices)
    builder.weights.ix[1] = pd.Series({"A": 0.5, "B": 0.5})
    builder.weights.ix[2] = pd.Series({"A": 0.3, "B": 0.7})
    portfolio = builder.build()
    print(portfolio.weights)
    print(sorted(builder.prices.columns.tolist()))