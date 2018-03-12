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
    dynamic = 0
    static = 1
    other = 2


# make Symbolgroup an enum


class Field(Base):
    __tablename__ = "reference_field"
    _id = Column("id", Integer, primary_key=True, autoincrement=True)
    name = Column(String(50), unique=True)
    type = Column(Enum(FieldType))

    def __repr__(self):
        return "{name}".format(name=self.name)

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self.name == other.name and self.type == other.type

    _symbols = relationship("_SymbolReference", collection_class=attribute_mapped_collection('symbol.bloomberg_symbol'),
                            back_populates="field")

    @property
    def data(self):
        return pd.Series({name: c.content for name, c in self._symbols.items()})


class SymbolGroup(Base):
    __tablename__ = "symbolsapp_group"
    _id = Column("id", Integer, primary_key=True, autoincrement=True)
    name = Column(String(50), unique=True)
    symbols = relationship("Symbol", back_populates="group")

    def __repr__(self):
        return "{name}".format(name=self.name)


class Symbol(Base):
    __tablename__ = "symbolsapp_symbol"
    _id = Column("id", Integer, primary_key=True, autoincrement=True)
    bloomberg_symbol = Column(String(50), unique=True)

    _group_id = Column("group_id", Integer, ForeignKey('symbolsapp_group.id'), nullable=True)
    group = relationship(SymbolGroup, back_populates="symbols")

    internal = Column(String, nullable=True)

    _timeseries = relationship("_Timeseries", collection_class=attribute_mapped_collection('name'),
                               cascade="all, delete-orphan")

    # _ref = relationship("_SymbolReference", collection_class=attribute_mapped_collection('field.name'),
    #                   cascade="all, delete-orphan", back_populates="field")

    fields = relationship("_SymbolReference", collection_class=attribute_mapped_collection('field.name'),
                          back_populates="symbol")

    @property
    def reference(self):
        return pd.Series({key: x.content for key, x in self.fields.items()})

    def __repr__(self):
        return "{name}, {group}".format(name=self.bloomberg_symbol, group=self.group)

    def update_reference(self, field, value):
        if field.name not in self.fields.keys():
            a = _SymbolReference(content=value)
            self.fields[field.name] = a
            field._symbols[self.bloomberg_symbol] = a

        else:
            self.fields[field.name].content = value

        return self.fields[field.name]

    # use enum instead of name
    def upsert_timeseries(self, name, ts):
        if name not in self._timeseries.keys():
            self._timeseries[name] = _Timeseries(name=name, symbol=self)

        self._timeseries[name].upsert(ts=ts)

    @property
    def get_timeseries(self):
        return pd.DataFrame({x.name: x.series for x in self._timeseries.values()})

    def get_timseries_by_name(self, name="PX_LAST"):
        if name in self._timeseries.keys():
            return self._timeseries[name].series
        else:
            return pd.Series({})


class _SymbolReference(Base):
    # http://docs.sqlalchemy.org/en/latest/orm/basic_relationships.html#many-to-many

    __tablename__ = 'symbolsapp_reference_data'

    _field_id = Column("field_id", Integer, ForeignKey(Field._id), primary_key=True)
    field = relationship(Field, back_populates="_symbols")

    _symbol_id = Column("symbol_id", Integer, ForeignKey(Symbol._id), primary_key=True)
    symbol = relationship(Symbol, back_populates="fields")

    content = Column(String(50))

    def __repr__(self):
        return "{symbol}, {field}, {value}".format(symbol=self.symbol, field=self.field, value=self.content)


class _Timeseries(Base):
    __tablename__ = 'ts_name'
    _id = Column("id", Integer, primary_key=True, autoincrement=True)
    # todo: make this enum
    name = Column(String(50))
    _symbol_id = Column("symbol_id", Integer, ForeignKey('symbolsapp_symbol.id'))
    symbol = relationship("Symbol", back_populates="_timeseries")

    _data = relationship("_TimeseriesData", collection_class=attribute_mapped_collection('date'), back_populates="ts",
                         cascade="all, delete-orphan")
    UniqueConstraint('symbol', 'name')

    def __init__(self, name, symbol):
        self.name = name
        self.symbol = symbol

    @property
    def series(self):
        return pd.Series({pd.Timestamp(date): x.value for date, x in self._data.items()})

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
                self._data[date] = _TimeseriesData(date=date, value=value, _ts_id=self._id)


class _TimeseriesData(Base):
    __tablename__ = 'ts_data'
    _id = Column("id", Integer, primary_key=True, autoincrement=True)
    date = Column(Date)
    value = Column(Float)
    _ts_id = Column("ts_id", Integer, ForeignKey('ts_name.id'))
    ts = relationship("_Timeseries", back_populates="_data")
    UniqueConstraint("date", "ts")


# class _TimeseriesData2(Base):
#    __tablename__ = 'ts_data2'
# enum for type PX LAST
# Symbol_id
# date
# value

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
    def read(x):
        json_str = BytesIO(x).read().decode()
        return pd.read_json(json_str, orient="split")

    @property
    def portfolio(self):
        return Portfolio(weights=self.weight, prices=self.price)

    @property
    def weight(self):
        try:
            return self.read(self._weights)
        except ValueError:
            return pd.DataFrame({})

    @property
    def price(self):
        try:
            return self.read(self._prices)
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

    def upsert(self, portfolio, days=5):
        if not self.portfolio:
            self.portfolio = PortfolioSQL(name=self.name, strategy=self)

        if self.portfolio.last_valid:
            # this is tricky. as the portfolio object may not contain an index yet...
            last_valid = self.portfolio.last_valid
            # update the existing portfolio object, think about renaming upsert into update...
            self.portfolio.upsert(portfolio=portfolio.truncate(before=last_valid - pd.DateOffset(days=days)))

        else:
            self.portfolio.upsert(portfolio=portfolio)


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
