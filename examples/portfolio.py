#!/usr/bin/env python
import pandas as pd
import matplotlib

matplotlib.use('Agg')

from pyutil.portfolio.portfolio import Portfolio


if __name__ == '__main__':
    pd.options.display.width = 300 #set_option("display.width", 300)

    import matplotlib.pyplot as plt
    plt.style.use('ggplot')

    file_prices = "data/price.csv"
    file_weights = "data/weight.csv"

    prices = pd.read_csv(file_prices, index_col=0, parse_dates=True)
    weights = pd.read_csv(file_weights, index_col=0, parse_dates=True)

    portfolio = Portfolio(prices, weights)
    f = portfolio.plot()

    print(100 * portfolio.nav.monthlytable)

    f.set_size_inches(15,15)
    print(f.get_axes())
    #print(f.suptitle("Test Portfolio"))
    f.savefig("/pyutil/examples/portfolio.png")
