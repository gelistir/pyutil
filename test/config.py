import os
import pandas as pd

from pyutil.portfolio.portfolio import Portfolio
from pyutil.performance.summary import NavSeries



def resource(name):
    base_dir = os.path.dirname(__file__)
    return os.path.join(base_dir, "resources", name)


def read_frame(name, parse_dates=True, index_col=0):
    return pd.read_csv(resource(name), index_col=index_col, header=0, parse_dates=parse_dates)


def read_series(name, parse_dates=True, index_col=0, cname=None):
    return pd.read_csv(resource(name), index_col=index_col, header=None, squeeze=True, parse_dates=parse_dates, names=[cname])


def test_portfolio():
    return Portfolio(prices=read_frame("price.csv"), weights=read_frame("weight.csv"))


def test_portfolio2():
    p = read_frame("price.csv")


def series2arrays(x, tz="CET"):
    # this function converts a pandas series into a dictionary of two arrays
    # to mock the behaviour of highcharts...
    def __f(x):
        return pd.Timestamp(x, tz=tz).value * 1e-6

    return {"time": [__f(key) for key in x.index], "data": [float(a) for a in x.values]}


if __name__ == '__main__':
    ts = NavSeries(read_series("ts.csv"))
    ts.drawdown.to_csv(resource("drawdown.csv"))

    ts.summary(alpha=0.95).to_csv(resource("summary.csv"))
    ts.monthlytable.to_csv(resource("monthtable.csv"))

    ts.period_returns.to_csv(resource("periods.csv"))


