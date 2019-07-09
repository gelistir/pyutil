from pyutil.strategy.config import ConfigMaster
from test.config import test_portfolio


class Configuration(ConfigMaster):
    def __init__(self, reader=None, **kwargs):
        super().__init__(names=test_portfolio().assets, reader=reader, **kwargs)
        self.__reader = reader
        self.__portfolio = test_portfolio()

    @property
    def portfolio(self):
        return self.__portfolio

