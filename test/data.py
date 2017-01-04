import os
import pandas as pd

from pyutil.performance.summary import NavSeries

BASE_DIR = os.path.dirname(__file__)


def __f(name):
    BASE_DIR = os.path.dirname(__file__)
    return os.path.join(BASE_DIR, "resources", name)

def read_series(name, parse_dates=True, index_col=0):
    return pd.read_csv(__f(name), index_col=index_col, header=None, squeeze=True, parse_dates=parse_dates)


if __name__ == '__main__':
    ts = NavSeries(read_series("ts.csv"))
    ts.drawdown.to_csv("resources/drawdown.csv")

    ts.summary(alpha=0.95).to_csv("resources/summary.csv")
    ts.monthlytable.to_csv("resources/monthtable.csv")

    ts.period_returns.to_csv("resources/periods.csv")

