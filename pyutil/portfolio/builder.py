import pandas as pd
import numpy as np

from pyutil.portfolio.portfolio import Portfolio


class PortfolioBuilder(object):
    def __init__(self, prices):
        self.__prices = prices.ffill()
        self.__weights = pd.DataFrame(index = prices.index, columns=prices.keys(), data=np.nan)

    @property
    def assets(self):
        return self.__prices.keys()

    @property
    def returns(self):
        return self.__prices.pct_change()

    # make sure weight is a dictionary or series!
    def set_weights(self, t, weights):
        for asset, weight in weights.items():
            self.set_weight(t, asset, weight)

    def set_weight(self, t, asset, weight):
        # we should have prices for the assets
        assert asset in self.assets, "Unknown asset {0}".format(asset)

        # the weight has to be a real number
        assert np.isfinite(weight), "Weight has to be a finite number {0}".format(weight)

        # the price has to be there
        assert np.isfinite(self.__prices[asset][t]), "Price for asset {0} has to be available at time {1}".format(asset, t)

        self.__weights[asset][t] = weight

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
        w1 = self.__weights.ix[p.index[-2]]

        # we only update weights that are finite
        w1 = w1[w1.notnull()]

        cash = 1.0 - w1.sum()
        value = w1 * (p2 / p1)
        w = value / (value.sum() + cash)

        for asset, weight in w.items():
            self.set_weight(t, asset=asset, weight=weight)


if __name__ == '__main__':
    prices = pd.DataFrame(columns=["A", "B"], index=[1, 2], data=[[100,100],[110,100]])
    print(prices)

    builder = PortfolioBuilder(prices=prices)
    builder.set_weight(t=1, asset="A", weight=0.2)
    builder.set_weight(t=1, asset="B", weight=0.8)

    builder.forward(t=2)

    portfolio = builder.build()
    print(portfolio.weights)


