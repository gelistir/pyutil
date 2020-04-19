import pandas as pd
from mongoengine import *

from pyutil.strategy.config import ConfigMaster
from .pandasdocument import PandasDocument
from pyutil.portfolio.portfolio import Portfolio
import os


def strategies(folder):
    for file in os.listdir(folder):
        with open(os.path.join(folder, file), "r") as f:
            source = f.read()
            m = _module(source=source)
            yield m.name, source


def _module(source):
    # We store the source code directly as a string in a mongo database!
    # Using reflection we get back to a module
    from types import ModuleType

    compiled = compile(source, '', 'exec')
    mod = ModuleType("module")
    exec(compiled, mod.__dict__)
    return mod


def configuration(strategy, reader=None):
    return strategy.module.Configuration(reader=reader)


class Strategy(PandasDocument):
    # active flag, only active strategies are updated
    active = BooleanField(default=True)
    # source code
    source = StringField()
    # type of the strategy (you can use whatever name here)
    type = StringField(max_length=100)

    def configuration(self, reader=None) -> ConfigMaster:
        return self.module.Configuration(reader=reader)

    @property
    def portfolio(self):
        try:
            return Portfolio(prices=self.prices, weights=self.weights)
        except AttributeError:
            return None

    @property
    def module(self):
        return _module(self.source)

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
    def reference_frame(cls, products=None) -> pd.DataFrame:
        products = products or Strategy.objects
        frame = super().reference_frame(products=products)
        frame["source"] = pd.Series({s.name: s.source for s in products})
        frame["type"] = pd.Series({s.name: s.type for s in products})
        frame["active"] = pd.Series({s.name: s.active for s in products})
        return frame

    @staticmethod
    def portfolios(strategies=None) -> dict:
        s = strategies or Strategy.objects
        return {strategy.name: strategy.portfolio for strategy in s if strategy.portfolio is not None}

    @staticmethod
    def navs(strategies=None) -> pd.DataFrame:
        frame = pd.DataFrame({key: item.nav for key, item in Strategy.portfolios(strategies).items()})
        frame.index.name = "Portfolio"
        return frame