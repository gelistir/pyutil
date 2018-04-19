import enum as _enum
from io import BytesIO as _BytesIO

import pandas as _pd
import sqlalchemy as sq
from sqlalchemy.orm import relationship as _relationship
from sqlalchemy.types import Enum as _Enum

from pyutil.portfolio.portfolio import Portfolio as _Portfolio
from pyutil.sql.base import Base
from pyutil.sql.products import ProductInterface


class Assets(list):
    def __init__(self, seq=()):
        super().__init__(seq)

    @property
    def reference(self):
        return _pd.DataFrame({asset.bloomberg_symbol: _pd.Series(asset.reference) for asset in self}).transpose()

    @property
    def internal(self):
        return {asset.bloomberg_symbol: asset.internal for asset in self}
    @property
    def group(self):
        return {asset.bloomberg_symbol: asset.group.name for asset in self}

    @property
    def group_internal(self):
        return _pd.DataFrame({"Group": _pd.Series(self.group), "Internal": _pd.Series(self.internal)})



class SymbolType(_enum.Enum):
    alternatives = "Alternatives"
    fixed_income = "Fixed Income"
    currency = "Currency"
    equities = "Equities"


class StrategyType(_enum.Enum):
    mdt = 'mdt'
    conservative = 'conservative'
    balanced = 'balanced'
    dynamic = 'dynamic'

# Make association table!
# Make Portfolio a ProductInterface

class Symbol(ProductInterface):
    __tablename__ = "symbolsapp_symbol"
    _id = sq.Column("id", sq.Integer, sq.ForeignKey(ProductInterface.id), primary_key=True)

    bloomberg_symbol = sq.Column(sq.String(50), unique=True)
    group = sq.Column("gg", _Enum(SymbolType))
    internal = sq.Column(sq.String, nullable=True)

    __mapper_args__ = {"polymorphic_identity": "symbol"}

    def __repr__(self):
        return "{name}".format(name=self.bloomberg_symbol)

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self.bloomberg_symbol == other.bloomberg_symbol

    def __hash__(self):
        return hash(str(self.bloomberg_symbol))


class PortfolioSQL(Base):
    __tablename__ = 'portfolio'
    name = sq.Column(sq.String, primary_key=True)
    _weights = sq.Column("weights", sq.LargeBinary)
    _prices = sq.Column("prices", sq.LargeBinary)
    _strategy_id = sq.Column("strategy_id", sq.Integer, sq.ForeignKey("strategiesapp_strategy.id"), nullable=True)
    strategy = _relationship("Strategy", back_populates="_portfolio")

    @property
    def empty(self):
        return self.weight.empty and self.price.empty

    @staticmethod
    def _read(x):
        json_str = _BytesIO(x).read().decode()
        return _pd.read_json(json_str, orient="split")

    @property
    def portfolio(self):
        return _Portfolio(weights=self.weight, prices=self.price)

    @property
    def weight(self):
        try:
            return self._read(self._weights)
        except ValueError:
            return _pd.DataFrame({})

    @property
    def price(self):
        try:
            return self._read(self._prices)
        except ValueError:
            return _pd.DataFrame({})

    @price.setter
    def price(self, value):
        self._prices = value.to_json(orient="split", date_format="iso").encode()

    @weight.setter
    def weight(self, value):
        self._weights = value.to_json(orient="split", date_format="iso").encode()

    @property
    def last_valid(self):
        try:
            return self.portfolio.index[-1]
        except:
            return None

    @property
    def assets(self):
        return self.portfolio.assets

    @property
    def nav(self):
        return self.portfolio.nav

    def sector(self, map, total=False):
        return self.portfolio.sector_weights(symbolmap=map, total=total)

    def upsert(self, portfolio):
        start = portfolio.index[0]
        p = self.price.truncate(after=start - _pd.DateOffset(seconds=1))
        w = self.weight.truncate(after=start - _pd.DateOffset(seconds=1))
        self.weight = _pd.concat([w, portfolio.weights], axis=0)
        self.price = _pd.concat([p, portfolio.prices], axis=0)
        return self


class Strategy(Base):
    __tablename__ = "strategiesapp_strategy"

    _id = sq.Column("id", sq.Integer, primary_key=True, autoincrement=True)
    name = sq.Column(sq.String(50), unique=True)
    active = sq.Column(sq.Boolean)
    source = sq.Column(sq.String)
    _portfolio = _relationship("PortfolioSQL", uselist=False, back_populates="strategy")
    type = sq.Column(_Enum(StrategyType))

    def __init__(self, name, active=True, source=""):
        self.name = name
        self._portfolio = PortfolioSQL(name=self.name, strategy=self)
        self.active = active
        self.source = source

    def __module(self):
        from types import ModuleType

        compiled = compile(self.source, '', 'exec')
        module = ModuleType("module")
        exec(compiled, module.__dict__)
        return module

    def compute_portfolio(self, reader):
        config = self.__module().Configuration(reader=reader)
        return config.portfolio

    @property
    def assets(self):
        return self._portfolio.assets

    def upsert(self, portfolio, days=0):
        if self._portfolio.last_valid:
            # this is tricky. as the portfolio object may not contain an index yet...
            last_valid = self._portfolio.last_valid
            # update the existing portfolio object, think about renaming upsert into update...
            self._portfolio.upsert(portfolio=portfolio.truncate(before=last_valid - _pd.DateOffset(days=days)))
        else:
            self._portfolio.upsert(portfolio=portfolio)

        return self._portfolio.portfolio

    @property
    def portfolio(self):
        return self._portfolio.portfolio

    @portfolio.setter
    def portfolio(self, portfolio):
        self._portfolio.upsert(portfolio=portfolio)


