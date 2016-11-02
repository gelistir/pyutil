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

    def yesterday_prices(self, t):
        p = self.__prices.truncate(after=t)
        return p.ix[p.index[-2]]

    def yesterday_weights(self, t):
        w = self.weights.truncate(after=t)
        return w.ix[w.index[-2]]

    @property
    def cash(self):
        return self.weights.sum(axis=1)

    @property
    def prices(self):
        return self.__prices

    def build(self, logger=None):
        return Portfolio(prices = self.__prices, weights = self.__weights, logger=logger)

    def forward(self, t):
        # We move weights to t
        p2 = self.current_prices(t)
        p1 = self.yesterday_prices(t)
        w1 = self.yesterday_weights(t)

        # we only update weights that are finite
        w1 = w1.dropna()

        # fraction of the cash in the portfolio yesterday
        cash = 1.0 - w1.sum()
        # new value of each position
        value = w1 * (p2 / p1)

        # new weights
        w = value / (value.sum() + cash)

        self.weights.ix[t] = w


if __name__ == '__main__':
    prices = pd.DataFrame(columns=["A", "B"], index=[1, 2], data=[[100,100],[110,100]])
    print(prices)

    builder = PortfolioBuilder(prices=prices)

    # set the initial weights...
    # builder.t = 1
    builder.weights.ix[1] = {"A": 0.2, "B": 0.8}

    print(builder.cash)
    print(builder.weights)

    for t in builder.timestamps[1:]:
        # forward weights from previous state
        builder.forward(t)

        # set new weights
        print(builder.weights)
        print(builder.current_weights)

    portfolio = builder.build()
    print(portfolio.weights)


