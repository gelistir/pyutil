import enum as _enum
import pandas as pd

import sqlalchemy as sq
from sqlalchemy.orm import relationship as _relationship
from sqlalchemy.types import Enum as _Enum
from pyutil.sql.interfaces.products import ProductInterface, Base


from pyutil.portfolio.portfolio import Portfolio as _Portfolio


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



_association_table = sq.Table('association', Base.metadata,
                            sq.Column('symbol_id', sq.Integer, sq.ForeignKey('symbolsapp_symbol.id')),
                            sq.Column('portfolio_id', sq.Integer, sq.ForeignKey('portfolio2.id'))
                               )

class Symbol(ProductInterface):
    __tablename__ = "symbolsapp_symbol"
    _id = sq.Column("id", sq.Integer, sq.ForeignKey(ProductInterface.id), primary_key=True)

    bloomberg_symbol = sq.Column(sq.String(50), unique=True)
    group = sq.Column("gg", _Enum(SymbolType))
    internal = sq.Column(sq.String, nullable=True)
    portfolio = _relationship("Portfolio", secondary=_association_table, back_populates="symbols")

    __mapper_args__ = {"polymorphic_identity": "symbol"}

    def __repr__(self):
        return "({name})".format(name=self.bloomberg_symbol)

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self.bloomberg_symbol == other.bloomberg_symbol

    def __hash__(self):
        return hash(str(self.bloomberg_symbol))

    def __lt__(self, other):
        return self.bloomberg_symbol < other.bloomberg_symbol


class Portfolio(ProductInterface):
    __tablename__ = 'portfolio2'
    _id = sq.Column("id", sq.Integer, sq.ForeignKey(ProductInterface.id), primary_key=True)

    _strategy_id = sq.Column("strategy_id", sq.Integer, sq.ForeignKey("strategiesapp_strategy.id"), nullable=True)
    strategy = _relationship("Strategy", back_populates="_portfolio")

    __mapper_args__ = {"polymorphic_identity": "portfolio"}
    symbols = _relationship(Symbol, secondary=_association_table, back_populates="portfolio")
    name = sq.Column(sq.String, unique=True)

    def upsert_price(self, symbol, data):
        if symbol not in self.symbols:
            self.symbols.append(symbol)
        self.upsert_ts(name="price", data=data, secondary=symbol)

    def upsert_weight(self, symbol, data):
        if symbol not in self.symbols:
            self.symbols.append(symbol)
        self.upsert_ts(name="weight", data=data, secondary=symbol)

    @property
    def empty(self):
        return self.frame(name="price").empty and self.frame(name="weight").empty

    def upsert(self, portfolio, assets=None):
        for name, data in portfolio.weights.items():
            self.upsert_weight(assets[name], data.dropna())

        for name, data in portfolio.prices.items():
            self.upsert_price(assets[name], data.dropna())

        return self

    @property
    def portfolio(self):
        # does it work?
        return _Portfolio(prices=self.price, weights=self.weight)

    @property
    def last_valid(self):
        try:
            return self.portfolio.index[-1]
        except:
            return None

    @property
    def weight(self):
        return self.frame(name="weight")

    @property
    def price(self):
        return self.frame(name="price")

    @property
    def nav(self):
        return self.portfolio.nav

    @property
    def sector(self):
        map = {asset: asset.group.name for asset in self.symbols}
        return self.portfolio.sector_weights(symbolmap=map, total=False)

    @property
    def sector_tail(self):
        map = {asset: asset.group.name for asset in self.symbols}
        w = self.portfolio.sector_weights(symbolmap=map, total=False)
        return w.loc[w.index[-1]]

    @property
    def assets2(self):
        return self.portfolio.assets

    def __lt__(self, other):
        return self.name < other.name



class Strategy(Base):
    __tablename__ = "strategiesapp_strategy"

    _id = sq.Column("id", sq.Integer, primary_key=True, autoincrement=True)
    name = sq.Column(sq.String(50), unique=True)
    active = sq.Column(sq.Boolean)
    source = sq.Column(sq.String)
    _portfolio = _relationship(Portfolio, uselist=False, back_populates="strategy")
    type = sq.Column(_Enum(StrategyType))

    def __init__(self, name, active=True, source=""):
        self.name = name
        self._portfolio = Portfolio(name=self.name, strategy=self)
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
            self._portfolio.upsert(portfolio=portfolio.truncate(before=last_valid - pd.DateOffset(days=days)))
        else:
            self._portfolio.upsert(portfolio=portfolio)

        return self._portfolio.portfolio

    @property
    def portfolio(self):
        return self._portfolio.portfolio

    @portfolio.setter
    def portfolio(self, portfolio):
        self._portfolio.upsert(portfolio=portfolio)
