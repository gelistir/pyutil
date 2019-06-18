import enum as _enum
import os
import sqlalchemy as sq
from sqlalchemy.types import Enum as _Enum

from pyutil.mongo.xportfolio import write_portfolio, read_portfolio
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


def strategies_from_db(session, reader=None):
    """ this opens the door for parallel execution """
    strategies = session.query(Strategy).filter(Strategy.active).all()
    for strategy in strategies:
        yield strategy, strategy.configuration(reader=reader)


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

    def read_portfolio(self, collection):
        return read_portfolio(collection=collection, name=self.name)

    def write_portfolio(self, portfolio, collection):
        write_portfolio(collection=collection, name=self.name, portfolio=portfolio)

    def assets(self):
        return self.configuration(reader=None).names


    # def upsert(self, portfolio, symbols=None, days=0):
    #     assert isinstance(portfolio, _Portfolio)
    #
    #     assert self._portfolio
    #
    #     # find the last stamp of weights...
    #     last = self.last
    #
    #     if not last:
    #         self._portfolio.upsert(portfolio=portfolio, symbols=symbols)
    #     else:
    #         # We only take the last few days of the new portfolio
    #         p1 = portfolio.truncate(before=last - pd.DateOffset(days=days))
    #         self._portfolio.upsert(portfolio=p1, symbols=symbols)
    #
    #     return self.portfolio

    # @property
    # def portfolio(self):
    #     return self._portfolio.portfolio
    #
    # @property
    # def last(self):
    #     return self._portfolio.last
    #
    # @property
    # def assets(self):
    #     return self._portfolio.symbols
    #
    # @property
    # def state(self):
    #     x = self._portfolio.state
    #     # print(x)
    #     y = self.reference_assets.drop(columns=["Name", "Sector"])
    #     y.index = [asset.name for asset in y.index]
    #     return pd.concat((x, y), axis=1, sort=False)
    #
    # @property
    # def reference_assets(self):
    #     return Symbol.reference_frame(symbols=self.assets)
    #
    # def sector(self, total=False):
    #     return self._portfolio.sector(total=total)
    #
    # @property
    # def symbols(self):
    #     return self._portfolio.symbols
