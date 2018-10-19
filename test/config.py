import os

import pandas as pd

from pyutil.performance.summary import fromNav
from pyutil.portfolio.portfolio import Portfolio

pd.options.display.width = 300

def resource(name):
    base_dir = os.path.dirname(__file__)
    return os.path.join(base_dir, "resources", name)


def read_frame(name, parse_dates=True, index_col=0):
    return pd.read_csv(resource(name), index_col=index_col, header=0, parse_dates=parse_dates)


def read_series(name, parse_dates=True, index_col=0, cname=None):
    return pd.read_csv(resource(name), index_col=index_col, header=None, squeeze=True, parse_dates=parse_dates,
                       names=[cname])


def test_portfolio():
    return Portfolio(prices=read_frame("price.csv"), weights=read_frame("weight.csv"))


if __name__ == '__main__':
    ts = fromNav(read_series("ts.csv"))
    ts.drawdown.to_csv(resource("drawdown.csv"))

    ts.summary(alpha=0.95).to_csv(resource("summary.csv"))
    ts.monthlytable.to_csv(resource("monthtable.csv"))

    ts.period_returns.to_csv(resource("periods.csv"))
