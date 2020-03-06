import os
import pandas as pd
import pytest

from pyutil.config.random import random_string
#from pyutil.mongo.mongo import Mongo
from pyutil.portfolio.portfolio import Portfolio


def resource(name):
    return os.path.join(os.path.dirname(__file__), "resources", name)


def read(name, **kwargs):
    return pd.read_csv(resource(name), **kwargs)


@pytest.fixture()
def portfolio():
    return Portfolio(prices=read("price.csv", parse_dates=True, index_col=0),
                     weights=read("weight.csv", parse_dates=True, index_col=0))


#@pytest.fixture()
#def mongo():
#    return Mongo(db=random_string(5), host="mongomock://localhost")

#MONGODB = {'db': random_string(5), 'host': "mongomock://localhost"}


from mongoengine import *
client = connect(db=random_string(5), host="mongomock://localhost")
