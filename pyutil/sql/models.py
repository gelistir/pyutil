from io import BytesIO as _BytesIO

import pandas as _pd
from sqlalchemy.ext.declarative import declarative_base as _declarative_base
from sqlalchemy.orm.collections import attribute_mapped_collection as _attribute_mapped_collection

from pyutil.portfolio.portfolio import Portfolio as _Portfolio

_Base = _declarative_base()

import sqlalchemy as sq
from sqlalchemy.orm import relationship as _relationship
from sqlalchemy.types import Enum as _Enum

import enum as _enum


class FieldType(_enum.Enum):
    dynamic = "dynamic"
    static = "static"
    other = "other"


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


class Field(_Base):
    __tablename__ = "reference_field"
    _id = sq.Column("id", sq.Integer, primary_key=True, autoincrement=True)
    name = sq.Column(sq.String(50), unique=True)
    type = sq.Column(_Enum(FieldType))

    def __repr__(self):
        return "{name}".format(name=self.name)

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self.name == other.name and self.type == other.type

    symbols = _relationship("_SymbolReference", collection_class=_attribute_mapped_collection('symbol.bloomberg_symbol'),
                           back_populates="field")


class Symbol(_Base):
    __tablename__ = "symbolsapp_symbol"
    _id = sq.Column("id", sq.Integer, primary_key=True, autoincrement=True)

    bloomberg_symbol = sq.Column(sq.String(50), unique=True)
    group = sq.Column("gg", _Enum(SymbolType))
    internal = sq.Column(sq.String, nullable=True)

    timeseries = _relationship("Timeseries", collection_class=_attribute_mapped_collection('name'),
                               cascade="all, delete-orphan")

    fields = _relationship("_SymbolReference", collection_class=_attribute_mapped_collection('field.name'),
                          back_populates="symbol")

    @property
    def reference(self):
        return _pd.Series({key: x.content for key, x in self.fields.items()})

    def __repr__(self):
        return "{name}".format(name=self.bloomberg_symbol)

    def update_reference(self, field, value):
        assert field
        assert value
        assert self.bloomberg_symbol

        # do not flush here!
        a = _SymbolReference(content=value)
        self.fields[field.name] = a
        field.symbols[self.bloomberg_symbol] = a
        return a

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self.bloomberg_symbol == other.bloomberg_symbol


class _SymbolReference(_Base):
    # http://docs.sqlalchemy.org/en/latest/orm/basic_relationships.html#many-to-many

    __tablename__ = 'reference_data'

    _field_id = sq.Column("field_id", sq.Integer, sq.ForeignKey(Field._id), primary_key=True)
    field = _relationship(Field, back_populates="symbols")

    _symbol_id = sq.Column("symbol_id", sq.Integer, sq.ForeignKey(Symbol._id), primary_key=True)
    symbol = _relationship(Symbol, back_populates="fields")

    content = sq.Column(sq.String(50))


class Timeseries(_Base):
    __tablename__ = 'ts_name'
    _id = sq.Column("id", sq.Integer, primary_key=True, autoincrement=True)
    # todo: make this enum
    name = sq.Column(sq.String(50))

    _symbol_id = sq.Column("symbol_id", sq.Integer, sq.ForeignKey('symbolsapp_symbol.id'))
    symbol = _relationship("Symbol", back_populates="timeseries")

    _data = _relationship("_TimeseriesData", collection_class=_attribute_mapped_collection('date'), back_populates="ts",
                         cascade="all, delete-orphan")

    sq.UniqueConstraint('symbol', 'name')

    @property
    def series(self):
        return _pd.Series({date: x.value for date, x in self._data.items()})

    def __repr__(self):
        return "{name} for {symbol}".format(name=self.name, symbol=self.symbol)

    @property
    def empty(self):
        return len(self._data) == 0

    @property
    def last_valid(self):
        if self.empty:
            return None
        else:
            return max(x for x in self._data.keys())

    def upsert(self, ts):
        for date, value in ts.items():
            #if date in self._data.keys():
            #    # thes is some data
            #    self._data[date].value = value
            #else:
            self._data[date] = _TimeseriesData(date=date, value=value) #, _ts_id=self._id)

        return self


class _TimeseriesData(_Base):
    __tablename__ = 'ts_data'
    date = sq.Column(sq.Date, primary_key=True)
    value = sq.Column(sq.Float)
    _ts_id = sq.Column("ts_id", sq.Integer, sq.ForeignKey('ts_name.id'), primary_key=True)
    ts = _relationship("Timeseries", back_populates="_data")



class PortfolioSQL(_Base):
    __tablename__ = 'portfolio'
    name = sq.Column(sq.String, primary_key=True)
    _weights = sq.Column("weights", sq.LargeBinary)
    _prices = sq.Column("prices", sq.LargeBinary)
    _strategy_id = sq.Column("strategy_id", sq.Integer, sq.ForeignKey("strategiesapp_strategy.id"), nullable=True)
    strategy = _relationship("Strategy", back_populates="portfolio")

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

    def sector(self, map):
        return self.portfolio.sector_weights(symbolmap=map, total=False)

    def upsert(self, portfolio):
        start = portfolio.index[0]
        p = self.price.truncate(after=start - _pd.DateOffset(seconds=1))
        w = self.weight.truncate(after=start - _pd.DateOffset(seconds=1))
        self.weight = _pd.concat([w, portfolio.weights], axis=0)
        self.price = _pd.concat([p, portfolio.prices], axis=0)


class Strategy(_Base):
    __tablename__ = "strategiesapp_strategy"

    _id = sq.Column("id", sq.Integer, primary_key=True, autoincrement=True)
    name = sq.Column(sq.String(50), unique=True)
    active = sq.Column(sq.Boolean)
    source = sq.Column(sq.String)
    portfolio = _relationship("PortfolioSQL", uselist=False, back_populates="strategy")
    type = sq.Column(_Enum(StrategyType))

    def __init__(self, name, active=True, source=""):
        self.name = name
        self.portfolio = PortfolioSQL(name=self.name, strategy=self)
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
        return self.portfolio.assets

    def upsert(self, portfolio, days=0):
        #if not self.portfolio:
        #    self.portfolio = PortfolioSQL(name=self.name, strategy=self)

        if self.portfolio.last_valid:
            # this is tricky. as the portfolio object may not contain an index yet...
            last_valid = self.portfolio.last_valid
            # update the existing portfolio object, think about renaming upsert into update...
            self.portfolio.upsert(portfolio=portfolio.truncate(before=last_valid - _pd.DateOffset(days=days)))

        else:
            self.portfolio.upsert(portfolio=portfolio)

        return self.portfolio.portfolio


class Frame(_Base):
    __tablename__ = 'frame'
    name = sq.Column(sq.String, primary_key=True)
    _data = sq.Column("data", sq.LargeBinary)
    _index = sq.Column("index", sq.String)

    @property
    def frame(self):
        json_str = _BytesIO(self._data).read().decode()
        return _pd.read_json(json_str, orient="split").set_index(keys=self._index.split(","))

    @frame.setter
    def frame(self, value):
        self._index = ",".join(value.index.names)
        self._data = value.reset_index().to_json(orient="split", date_format="iso").encode()
