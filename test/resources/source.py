import pandas as pd

from test.config import test_portfolio


class Configuration(object):
    def __init__(self, reader):
        self.__reader = reader
        self.__portfolio = test_portfolio()#.truncate(before=pd.Timestamp("2015-04-01"))

    @property
    def portfolio(self):
        return self.__portfolio

    @property
    def assets(self):
        return self.__portfolio.assets
