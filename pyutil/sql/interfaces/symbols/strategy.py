import enum as _enum
import os
import sqlalchemy as sq
from sqlalchemy.types import Enum as _Enum

from pyutil.portfolio.portfolio import Portfolio
from pyutil.sql.interfaces.products import ProductInterface
from pyutil.timeseries.merge import last_index


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
        prices = self.read(kind="PRICES")
        weights = self.read(kind="WEIGHTS")

        if prices is None and weights is None:
            return None
        else:
            return Portfolio(prices=prices, weights=weights)

    @portfolio.setter
    def portfolio(self, portfolio):
        self.write(data=portfolio.weights, kind="WEIGHTS")
        self.write(data=portfolio.prices, kind="PRICES")

    @property
    def assets(self):
        return self.configuration(reader=None).names

    @property
    def last(self):
        p = self.portfolio
        if p is None:
            return None
        return p.last

