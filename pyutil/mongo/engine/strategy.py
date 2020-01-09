import pandas as pd
from mongoengine import *
from .pandasdocument import PandasDocument
from pyutil.portfolio.portfolio import Portfolio
from pyutil.strategy.config import ConfigMaster


def _module(source):
    from types import ModuleType

    compiled = compile(source, '', 'exec')
    mod = ModuleType("module")
    exec(compiled, mod.__dict__)
    return mod


class Strategy(PandasDocument):
    active = BooleanField(default=True)
    source = StringField()
    type = StringField(max_length=100)

    def configuration(self, reader=None) -> ConfigMaster:
        # Configuration only needs a reader to access the symbols...
        # Reader is a function taking the name of an asset as a parameter
        return _module(self.source).Configuration(reader=reader)

    @property
    def portfolio(self):
        try:
            return Portfolio(prices=self.prices, weights=self.weights)
        except AttributeError:
            return None

    @portfolio.setter
    def portfolio(self, portfolio):
        self.weights = portfolio.weights
        self.prices = portfolio.prices

    @property
    def assets(self):
        return self.configuration(reader=None).names

    @property
    def last_valid_index(self):
        try:
            return self.prices.last_valid_index()
        except AttributeError:
            return None

    @classmethod
    def reference_frame(cls, products) -> pd.DataFrame:
        frame = PandasDocument.reference_frame(products=products)
        frame["source"] = pd.Series({s.name: s.source for s in products})
        frame["type"] = pd.Series({s.name: s.type for s in products})
        frame["active"] = pd.Series({s.name: s.active for s in products})
        frame.index.name = "strategy"
        return frame

    @staticmethod
    def portfolios(strategies) -> dict:
        return {strategy.name: strategy.portfolio for strategy in strategies if strategy.portfolio is not None}

    @staticmethod
    def navs(strategies) -> pd.DataFrame:
        frame = pd.DataFrame({key: item.nav for key, item in Strategy.portfolios(strategies).items()})
        frame.index.name = "Portfolio"
        return frame