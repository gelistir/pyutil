import os
import pandas as pd
import pytest

from pyutil.portfolio.portfolio import Portfolio


def resource(name):
    return os.path.join(os.path.dirname(__file__), "resources", name)


def read(name, **kwargs):
    return pd.read_csv(resource(name), **kwargs)


def test_portfolio():
    return Portfolio(prices=read("price.csv", parse_dates=True, index_col=0),
                     weights=read("weight.csv", parse_dates=True, index_col=0))

@pytest.fixture()
def mongo():
    from mongomock import MongoClient
    return MongoClient().test
