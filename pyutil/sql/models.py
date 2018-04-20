from io import BytesIO as _BytesIO

import pandas as _pd
import sqlalchemy as sq

from pyutil.portfolio.portfolio import Portfolio as _Portfolio
from pyutil.sql.base import Base


# _association_table = sq.Table('association', Base.metadata,
#                             sq.Column('symbol_id', sq.Integer, sq.ForeignKey('symbolsapp_symbol.id')),
#                             sq.Column('portfolio_id', sq.Integer, sq.ForeignKey('portfolio2.id'))
#                                )


# class SymbolType(_enum.Enum):
#     alternatives = "Alternatives"
#     fixed_income = "Fixed Income"
#     currency = "Currency"
#     equities = "Equities"


# class StrategyType(_enum.Enum):
#     mdt = 'mdt'
#     conservative = 'conservative'
#     balanced = 'balanced'
#     dynamic = 'dynamic'


# class Symbol(ProductInterface):
#     __tablename__ = "symbolsapp_symbol"
#     _id = sq.Column("id", sq.Integer, sq.ForeignKey(ProductInterface.id), primary_key=True)
#
#     bloomberg_symbol = sq.Column(sq.String(50), unique=True)
#     group = sq.Column("gg", _Enum(SymbolType))
#     internal = sq.Column(sq.String, nullable=True)
#     portfolio = _relationship("Portfolio", secondary=_association_table, back_populates="symbols")
#
#     __mapper_args__ = {"polymorphic_identity": "symbol"}
#
#     def __repr__(self):
#         return "{name}".format(name=self.bloomberg_symbol)
#
#     def __eq__(self, other):
#         return self.__class__ == other.__class__ and self.bloomberg_symbol == other.bloomberg_symbol
#
#     def __hash__(self):
#         return hash(str(self.bloomberg_symbol))
#
#
# class Portfolio(ProductInterface):
#     __tablename__ = 'portfolio2'
#     _id = sq.Column("id", sq.Integer, sq.ForeignKey(ProductInterface.id), primary_key=True)
#
#     _strategy_id = sq.Column("strategy_id", sq.Integer, sq.ForeignKey("strategiesapp_strategy.id"), nullable=True)
#     strategy = _relationship("Strategy", backref="_portfolio")
#
#     __mapper_args__ = {"polymorphic_identity": "portfolio"}
#     symbols = _relationship(Symbol, secondary=_association_table, back_populates="portfolio")
#     name = sq.Column(sq.String, unique=True)
#
#     def upsert_price(self, symbol, data):
#         self.upsert_ts(name="price", data=data, secondary=symbol)
#
#     def upsert_weight(self, symbol, data):
#         self.upsert_ts(name="weight", data=data, secondary=symbol)
#
#     @property
#     def empty(self):
#         return self.frame(name="price").empty and self.frame(name="weight").empty
#
#     def upsert(self, portfolio, assets=None):
#         for name, data in portfolio.weights.items():
#             print(name)
#             asset = assets[name]
#             print(asset)
#             self.upsert_weight(asset, data.dropna())
#
#         for name, data in portfolio.prices.items():
#             print(name)
#             asset = assets[name]
#             print(asset)
#
#             self.upsert_price(asset, data.dropna())
#
#         print(self._timeseries.keys())
#         return self
#
#     @property
#     def portfolio(self):
#         # does it work?
#         return _Portfolio(prices=self.price, weights=self.weight)
#
#     @property
#     def last_valid(self):
#         try:
#             return self.portfolio.index[-1]
#         except:
#             return None
#
#     @property
#     def weight(self):
#         return self.frame(name="weight")
#
#     @property
#     def price(self):
#         return self.frame(name="price")
#
#     @property
#     def nav(self):
#         return self.portfolio.nav
#
#     @property
#     def sector(self):
#         map = {asset: asset.group.name for asset in self.symbols}
#         return self.portfolio.sector_weights(symbolmap=map, total=False)
#
#     @property
#     def assets2(self):
#         return self.portfolio.assets


class PortfolioSQL(Base):
    __tablename__ = 'portfolio'
    name = sq.Column(sq.String, primary_key=True)
    _weights = sq.Column("weights", sq.LargeBinary)
    _prices = sq.Column("prices", sq.LargeBinary)
    #_strategy_id = sq.Column("strategy_id", sq.Integer, sq.ForeignKey("strategiesapp_strategy.id"), nullable=True)
    #strategy = _relationship("Strategy", back_populates="_portfolio")

    #@property
    #def empty(self):
    #    return self.weight.empty and self.price.empty

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

    # @price.setter
    # def price(self, value):
    #     self._prices = value.to_json(orient="split", date_format="iso").encode()
    #
    # @weight.setter
    # def weight(self, value):
    #     self._weights = value.to_json(orient="split", date_format="iso").encode()

    #@property
    #def last_valid(self):
    #    try:
    #        return self.portfolio.index[-1]
    #    except:
    #        return None

    @property
    def assets(self):
        return self.portfolio.assets

    #@property
    #def nav(self):
    #    return self.portfolio.nav

    #def sector(self, map, total=False):
    #    return self.portfolio.sector_weights(symbolmap=map, total=total)

    #def upsert(self, portfolio):
    #    start = portfolio.index[0]
    #    p = self.price.truncate(after=start - _pd.DateOffset(seconds=1))
    #    w = self.weight.truncate(after=start - _pd.DateOffset(seconds=1))
    #    self.weight = _pd.concat([w, portfolio.weights], axis=0)
    #    self.price = _pd.concat([p, portfolio.prices], axis=0)
    #    return self


# class Strategy(Base):
#     __tablename__ = "strategiesapp_strategy"
#
#     _id = sq.Column("id", sq.Integer, primary_key=True, autoincrement=True)
#     name = sq.Column(sq.String(50), unique=True)
#     active = sq.Column(sq.Boolean)
#     source = sq.Column(sq.String)
#     #_portfolio = _relationship(Portfolio, uselist=False, back_populates="strategy")
#     type = sq.Column(_Enum(StrategyType))
#
#     def __init__(self, name, active=True, source=""):
#         self.name = name
#         self._portfolio = PortfolioSQL(name=self.name, strategy=self)
#         self.active = active
#         self.source = source
#
#     def __module(self):
#         from types import ModuleType
#
#         compiled = compile(self.source, '', 'exec')
#         module = ModuleType("module")
#         exec(compiled, module.__dict__)
#         return module
#
#     def compute_portfolio(self, reader):
#         config = self.__module().Configuration(reader=reader)
#         return config.portfolio
#
#     @property
#     def assets(self):
#         return self._portfolio.assets
#
#     def upsert(self, portfolio, days=0):
#         if self._portfolio.last_valid:
#             # this is tricky. as the portfolio object may not contain an index yet...
#             last_valid = self._portfolio.last_valid
#             # update the existing portfolio object, think about renaming upsert into update...
#             self._portfolio.upsert(portfolio=portfolio.truncate(before=last_valid - _pd.DateOffset(days=days)))
#         else:
#             self._portfolio.upsert(portfolio=portfolio)
#
#         return self._portfolio.portfolio
#
#     @property
#     def portfolio(self):
#         return self._portfolio.portfolio
#
#     @portfolio.setter
#     def portfolio(self, portfolio):
#         self._portfolio.upsert(portfolio=portfolio)
#

