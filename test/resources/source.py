from pyutil.portfolio.portfolio import Portfolio
from pyutil.strategy.config import ConfigMaster
from test.config import read


def _portfolio():
    return Portfolio(prices=read("price.csv", parse_dates=True, index_col=0),
                     weights=read("weight.csv", parse_dates=True, index_col=0))


class Configuration(ConfigMaster):
    def __init__(self, reader=None, **kwargs):
        super().__init__(names=_portfolio().assets, reader=reader, **kwargs)
        self.__reader = reader
        self.__portfolio = _portfolio()

    @property
    def portfolio(self):
        return self.__portfolio

