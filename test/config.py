import os
import pandas as pd
from pyutil.portfolio.portfolio import Portfolio

BASE_DIR = os.path.dirname(__file__)


def __f(name):
    BASE_DIR = os.path.dirname(__file__)
    return os.path.join(BASE_DIR, "resources", name)


def read_frame(name, parse_dates=True, index_col=0):
    return pd.read_csv(__f(name), index_col=index_col, header=0, parse_dates=parse_dates)


def read_series(name, parse_dates=True, index_col=0):
    return pd.read_csv(__f( name), index_col=index_col, header=None, squeeze=True, parse_dates=parse_dates)


def test_portfolio():
    return Portfolio(prices=read_frame("price.csv"), weights=read_frame("weight.csv"))
