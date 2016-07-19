import pandas as pd
import os
import matplotlib

matplotlib.use('Agg')

import matplotlib.pyplot as plt
from pyutil.portfolio.portfolio import Portfolio


if __name__ == '__main__':
    pd.set_option("display.width", 300)

    BASEDIR = os.path.dirname(__file__)
    file_prices = os.path.join(BASEDIR, "test", "resources", "price.csv")
    file_weights = os.path.join(BASEDIR, "test", "resources", "weight.csv")

    prices = pd.read_csv(file_prices, index_col=0, parse_dates=True)
    weights = pd.read_csv(file_weights, index_col=0, parse_dates=True)

    portfolio = Portfolio(prices, weights)
    portfolio.plot()

    print(100 * portfolio.nav.monthlytable)

    plt.savefig("portfolio.png")
    plt.show()
