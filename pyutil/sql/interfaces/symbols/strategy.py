import enum as _enum
import os

import pandas as pd
import sqlalchemy as sq
from sqlalchemy.types import Enum as _Enum

from pyutil.portfolio.portfolio import Portfolio
from pyutil.sql.interfaces.products import ProductInterface


def _module(source):
    from types import ModuleType

    compiled = compile(source, '', 'exec')
    mod = ModuleType("module")
    exec(compiled, mod.__dict__)
    return mod


def strategies(folder):
    for file in os.listdir(folder):
        with open(os.path.join(folder, file), "r") as f:
            source = f.read()
            m = _module(source=source)
            yield m.name, source


class StrategyType(_enum.Enum):
    mdt = 'mdt'
    conservative = 'conservative'
    balanced = 'balanced'
    dynamic = 'dynamic'


StrategyTypes = {s.value: s for s in StrategyType}


class Strategy(ProductInterface):
    __searchable__ = ["name", "type"]
    active = sq.Column(sq.Boolean)
    source = sq.Column(sq.String)
    type = sq.Column(_Enum(StrategyType))

    def __init__(self, name, active=True, source="", type=StrategyType.conservative):
        super().__init__(name)
        self.active = active
        self.source = source
        self.type = type

    def configuration(self, reader=None):
        # Configuration only needs a reader to access the symbols...
        # Reader is a function taking the name of an asset as a parameter
        return _module(self.source).Configuration(reader=reader)

    @property
    def portfolio(self):
        prices = self.read(key="PRICES")
        weights = self.read(key="WEIGHTS")

        if prices is None and weights is None:
            return None
        else:
            return Portfolio(prices=prices, weights=weights)

    @portfolio.setter
    def portfolio(self, portfolio):
        self.write(data=portfolio.weights, key="WEIGHTS")
        self.write(data=portfolio.prices, key="PRICES")

    @property
    def assets(self):
        return self.configuration(reader=None).names

    @property
    def last_valid_index(self):
        return self.__collection__.last(key="PRICES", name=self.name)

    @staticmethod
    def reference_frame(strategies):
        frame = Strategy._reference_frame(products=strategies)
        frame["source"] = pd.Series({s: s.source for s in strategies})
        frame["type"] = pd.Series({s: s.type for s in strategies})
        frame["active"] = pd.Series({s: s.active for s in strategies})
        return frame
