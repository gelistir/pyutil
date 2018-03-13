from io import BytesIO
from types import ModuleType

import pandas as pd
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.collections import attribute_mapped_collection

from pyutil.portfolio.portfolio import Portfolio

Base = declarative_base()

from sqlalchemy import Column, Integer, String, ForeignKey, UniqueConstraint, Date, Float, LargeBinary, Boolean
from sqlalchemy.orm import relationship
from sqlalchemy.types import Enum

import enum


class FieldType(enum.Enum):
    dynamic = "dynamic"
    static = "static"
    other = "other"


class SymbolType(enum.Enum):
    alternatives = "Alternatives"
    fixed_income = "Fixed Income"
    currency = "Currency"
    equities = "Equities"


class StrategyType(enum.Enum):
    mdt = 'mdt'
    conservative = 'conservative'
    balanced = 'balanced'
    dynamic = 'dynamic'


class Field(Base):
    __tablename__ = "reference_field"
    _id = Column("id", Integer, primary_key=True, autoincrement=True)
    name = Column(String(50), unique=True)
    type = Column(Enum(FieldType))

    def __repr__(self):
        return "{name}".format(name=self.name)

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self.name == other.name and self.type == other.type

    symbols = relationship("_SymbolReference", collection_class=attribute_mapped_collection('symbol.bloomberg_symbol'),
                           back_populates="field")


class Symbol(Base):
    __tablename__ = "symbolsapp_symbol"
    _id = Column("id", Integer, primary_key=True, autoincrement=True)

    bloomberg_symbol = Column(String(50), unique=True)
    group = Column("gg", Enum(SymbolType))
    internal = Column(String, nullable=True)

    timeseries = relationship("Timeseries", collection_class=attribute_mapped_collection('name'),
                               cascade="all, delete-orphan")

    fields = relationship("_SymbolReference", collection_class=attribute_mapped_collection('field.name'),
                          back_populates="symbol")

    @property
    def reference(self):
        return pd.Series({key: x.content for key, x in self.fields.items()})

    def __repr__(self):
        return "{name}".format(name=self.bloomberg_symbol)

    def update_reference(self, field, value):
        if field.name not in self.fields.keys():
            # do not flush here!
            a = _SymbolReference(content=value)  # , _field_id=field._id, _symbol_id=self._id)

            self.fields[field.name] = a
            field.symbols[self.bloomberg_symbol] = a
        else:
            self.fields[field.name].content = value

        return self.fields[field.name]

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self.bloomberg_symbol == other.bloomberg_symbol


class _SymbolReference(Base):
    # http://docs.sqlalchemy.org/en/latest/orm/basic_relationships.html#many-to-many

    __tablename__ = 'reference_data'

    _field_id = Column("field_id", Integer, ForeignKey(Field._id), primary_key=True)
    field = relationship(Field, back_populates="symbols")

    _symbol_id = Column("symbol_id", Integer, ForeignKey(Symbol._id), primary_key=True)
    symbol = relationship(Symbol, back_populates="fields")

    content = Column(String(50))


class Timeseries(Base):
    __tablename__ = 'ts_name'
    _id = Column("id", Integer, primary_key=True, autoincrement=True)
    # todo: make this enum
    name = Column(String(50))

    _symbol_id = Column("symbol_id", Integer, ForeignKey('symbolsapp_symbol.id'))
    symbol = relationship("Symbol", back_populates="timeseries")

    _data = relationship("_TimeseriesData", collection_class=attribute_mapped_collection('date'), back_populates="ts",
                         cascade="all, delete-orphan")

    UniqueConstraint('symbol', 'name')

    @property
    def series(self):
        return pd.Series({date: x.value for date, x in self._data.items()})

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
            if date in self._data.keys():
                # thes is some data
                self._data[date].value = value
            else:
                self._data[date] = _TimeseriesData(date=date, value=value) #, _ts_id=self._id)

        return self


class _TimeseriesData(Base):
    __tablename__ = 'ts_data'
    date = Column(Date, primary_key=True)
    value = Column(Float)
    _ts_id = Column("ts_id", Integer, ForeignKey('ts_name.id'), primary_key=True)
    ts = relationship("Timeseries", back_populates="_data")



class PortfolioSQL(Base):
    __tablename__ = 'portfolio'
    name = Column(String, primary_key=True)
    _weights = Column("weights", LargeBinary)
    _prices = Column("prices", LargeBinary)
    _strategy_id = Column("strategy_id", Integer, ForeignKey("strategiesapp_strategy.id"), nullable=True)
    strategy = relationship("Strategy", back_populates="portfolio")

    @property
    def empty(self):
        return self.weight.empty and self.price.empty

    @staticmethod
    def _read(x):
        json_str = BytesIO(x).read().decode()
        return pd.read_json(json_str, orient="split")

    @property
    def portfolio(self):
        return Portfolio(weights=self.weight, prices=self.price)

    @property
    def weight(self):
        try:
            return self._read(self._weights)
        except ValueError:
            return pd.DataFrame({})

    @property
    def price(self):
        try:
            return self._read(self._prices)
        except ValueError:
            return pd.DataFrame({})

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
        p = self.price.truncate(after=start - pd.DateOffset(seconds=1))
        w = self.weight.truncate(after=start - pd.DateOffset(seconds=1))
        self.weight = pd.concat([w, portfolio.weights], axis=0)
        self.price = pd.concat([p, portfolio.prices], axis=0)


class Strategy(Base):
    __tablename__ = "strategiesapp_strategy"

    _id = Column("id", Integer, primary_key=True, autoincrement=True)
    name = Column(String(50), unique=True)
    active = Column(Boolean)
    source = Column(String)
    portfolio = relationship("PortfolioSQL", uselist=False, back_populates="strategy")
    type = Column(Enum(StrategyType))

    def __init__(self, name, active=True, source=""):
        self.name = name
        self.portfolio = PortfolioSQL(name=self.name, strategy=self)
        self.active = active
        self.source = source

    def __module(self):
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
            self.portfolio.upsert(portfolio=portfolio.truncate(before=last_valid - pd.DateOffset(days=days)))

        else:
            self.portfolio.upsert(portfolio=portfolio)

        return self.portfolio.portfolio


class Frame(Base):
    __tablename__ = 'frame'
    name = Column(String, primary_key=True)
    _data = Column("data", LargeBinary)
    _index = Column("index", String)

    def __init__(self, frame, name):
        self.name = name
        self.frame = frame

    @property
    def frame(self):
        json_str = BytesIO(self._data).read().decode()
        return pd.read_json(json_str, orient="split").set_index(keys=self._index.split(","))

    @frame.setter
    def frame(self, value):
        self._index = ",".join(value.index.names)
        self._data = value.reset_index().to_json(orient="split", date_format="iso").encode()
