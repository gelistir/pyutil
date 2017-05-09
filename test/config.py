import os
import pandas as pd

from pyutil.mongo.asset import Asset
from pyutil.portfolio.portfolio import Portfolio
from pyutil.performance.summary import NavSeries
from mongoengine import connect as connect_mongo

def resource(name):
    base_dir = os.path.dirname(__file__)
    return os.path.join(base_dir, "resources", name)


def read_frame(name, parse_dates=True, index_col=0):
    return pd.read_csv(resource(name), index_col=index_col, header=0, parse_dates=parse_dates)


def read_series(name, parse_dates=True, index_col=0, cname=None):
    return pd.read_csv(resource(name), index_col=index_col, header=None, squeeze=True, parse_dates=parse_dates, names=[cname])


def test_portfolio(**kwargs):
    return Portfolio(prices=read_frame("price.csv"), weights=read_frame("weight.csv"),**kwargs)

def test_asset(name="TEST FP Equity"):
    return Asset(name=name, data=read_frame("price.csv")["B"], **read_frame("symbols.csv").ix["B"].to_dict())

def connect():
    # this will connect to an empty database...
    db = connect_mongo('test', host="testmongo", alias="default")
    # oddly enough, this doesn't really work
    for name in db.database_names():
        db.drop_database(name)


if __name__ == '__main__':
    ts = NavSeries(read_series("ts.csv"))
    ts.drawdown.to_csv(resource("drawdown.csv"))

    ts.summary(alpha=0.95).to_csv(resource("summary.csv"))
    ts.monthlytable.to_csv(resource("monthtable.csv"))

    ts.period_returns.to_csv(resource("periods.csv"))


