import json
import os
import pandas as pd
import pytest

from pyutil.config.random import random_string
from pyutil.portfolio.portfolio import Portfolio


def resource(name):
    return os.path.join(os.path.dirname(__file__), "resources", name)


def read_pd(name, **kwargs):
    return pd.read_csv(resource(name), **kwargs)


@pytest.fixture()
def portfolio():
    return Portfolio(prices=read_pd("price.csv", parse_dates=True, index_col=0),
                     weights=read_pd("weight.csv", parse_dates=True, index_col=0))


from mongoengine import *
client = connect(db=random_string(5), host="mongomock://localhost")


def read_json(name):
    with open(resource(name)) as f:
        return json.load(f)

