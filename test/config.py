import os

import pandas as pd

from pyutil.performance.summary import fromNav
from pyutil.portfolio.portfolio import Portfolio
from pyutil.testing.aux import resource_folder

pd.options.display.width = 300

# this is a function mapping name of a file to its path...
resource = resource_folder(folder=os.path.dirname(__file__))


def read(name, index_col=0, parse_dates=False, header=0, squeeze=False, **kwargs):
    return pd.read_csv(resource(name), index_col=index_col, parse_dates=parse_dates, header=header, squeeze=squeeze, **kwargs)


def test_portfolio():
    return Portfolio(prices=read("price.csv", parse_dates=True), weights=read("weight.csv", parse_dates=True))


if __name__ == '__main__':
    ts = fromNav(read("ts.csv", squeeze=True, header=None, parse_dates=True))
    print(ts)

    ts.drawdown.to_csv(resource(name="drawdown.csv"))

    ts.summary(alpha=0.95).to_csv(resource("summary.csv"))
    ts.monthlytable.to_csv(resource("monthtable.csv"))

    ts.period_returns.to_csv(resource("periods.csv"))
