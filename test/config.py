import os
from contextlib import ExitStack

import pandas as pd
from pony import orm

from pyutil.portfolio.portfolio import Portfolio
from pyutil.performance.summary import NavSeries
from pyutil.sql.db import define_database


def resource(name):
    base_dir = os.path.dirname(__file__)
    return os.path.join(base_dir, "resources", name)


def read_frame(name, parse_dates=True, index_col=0):
    return pd.read_csv(resource(name), index_col=index_col, header=0, parse_dates=parse_dates)


def read_series(name, parse_dates=True, index_col=0, cname=None):
    return pd.read_csv(resource(name), index_col=index_col, header=None, squeeze=True, parse_dates=parse_dates, names=[cname])


def test_portfolio():
    return Portfolio(prices=read_frame("price.csv"), weights=read_frame("weight.csv"))


class TestEnv(ExitStack):
    def __init__(self, provider='sqlite', filename=":memory:"):
        super().__init__()
        self.__database = define_database(provider=provider, filename=filename)
        self.enter_context(orm.db_session())

    @property
    def database(self):
        return self.__database



if __name__ == '__main__':
    ts = NavSeries(read_series("ts.csv"))
    ts.drawdown.to_csv(resource("drawdown.csv"))

    ts.summary(alpha=0.95).to_csv(resource("summary.csv"))
    ts.monthlytable.to_csv(resource("monthtable.csv"))

    ts.period_returns.to_csv(resource("periods.csv"))


