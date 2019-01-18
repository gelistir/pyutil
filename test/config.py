import os

import pandas as pd

from pyutil.performance.summary import fromNav
from pyutil.portfolio.portfolio import Portfolio
from pyutil.test.aux import resource_folder, read_series, read_frame

pd.options.display.width = 300

# this is a function mapping name of a file to its path...
resource = resource_folder(folder=os.path.dirname(__file__))


def test_portfolio():
    return Portfolio(prices=read_frame(resource("price.csv")), weights=read_frame(resource("weight.csv")))

def read(name, **kwargs):
    return pd.read_csv(resource(name), **kwargs)

if __name__ == '__main__':
    ts = fromNav(read_series(file=resource("ts.csv")))
    print(ts)

    ts.drawdown.to_csv(resource(name="drawdown.csv"))

    ts.summary(alpha=0.95).to_csv(resource("summary.csv"))
    ts.monthlytable.to_csv(resource("monthtable.csv"))

    ts.period_returns.to_csv(resource("periods.csv"))
