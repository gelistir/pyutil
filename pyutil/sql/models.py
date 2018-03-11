from io import BytesIO
from types import ModuleType

import pandas as pd
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm.collections import attribute_mapped_collection

from pyutil.portfolio.portfolio import Portfolio

Base = declarative_base()

from sqlalchemy import Column, Integer, String, ForeignKey, UniqueConstraint, Date, Float, LargeBinary, Boolean
from sqlalchemy.orm import relationship


class Type(Base):
    __tablename__ = "symbolsapp_reference_type"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(50), unique=True)
    fields = relationship("Field", back_populates="type")

    def __repr__(self):
        return "Type: {type}".format(type=self.name)


class Field(Base):
    __tablename__ = "symbolsapp_reference_field"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(50), unique=True)

    _type_id = Column("type_id", Integer, ForeignKey('symbolsapp_reference_type.id'), nullable=True)
    type = relationship(Type, back_populates="fields")

    data = relationship("_SymbolReference", back_populates="field")

    def __repr__(self):
        return "Field: {name}, {type}".format(name=self.name, type=self.type)


class _SymbolReference(Base):
    __tablename__ = 'symbolsapp_reference_data'
    id = Column(Integer, primary_key=True, autoincrement=True)

    _field_id = Column("field_id", Integer, ForeignKey('symbolsapp_reference_field.id'), nullable=False)
    field = relationship(Field, back_populates="data")

    _symbol_id = Column("symbol_id", Integer, ForeignKey("symbolsapp_symbol.id"), nullable=False)
    symbol = relationship("Symbol", back_populates="_ref")

    content = Column(String(50))
    UniqueConstraint('symbol_id', 'field_id')

    def __repr__(self):
        return "{symbol}, {field}, Value: {value}".format(symbol=self.symbol, field=self.field, value=self.content)


class SymbolGroup(Base):
    __tablename__ = "symbolsapp_group"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(50), unique=True)
    symbols = relationship("Symbol", back_populates="group")

    def __repr__(self):
        return "Group: {name}".format(name=self.name)


class Symbol(Base):
    __tablename__ = "symbolsapp_symbol"
    id = Column(Integer, primary_key=True, autoincrement=True)
    bloomberg_symbol = Column(String(50), unique=True)

    _group_id = Column("group_id", Integer, ForeignKey('symbolsapp_group.id'), nullable=True)
    group = relationship(SymbolGroup, back_populates="symbols")

    internal = Column(String, nullable=True)

    timeseries = relationship("Timeseries", collection_class=attribute_mapped_collection('name'),
                              cascade="all, delete-orphan")
    _ref = relationship("_SymbolReference", collection_class=attribute_mapped_collection('field.name'),
                       cascade="all, delete-orphan")

    def __init__(self, bloomberg_symbol, group=None, timeseries=None):
        timeseries = timeseries or []
        self.bloomberg_symbol = bloomberg_symbol

        if group:
            self.group = group

        for t in timeseries:
            self.timeseries[t] = Timeseries(name=t, symbol=self)

    @property
    def reference(self):
        return pd.Series({key: x.content for key, x in self._ref.items()})

    def __repr__(self):
        return "Symbol: {name}, {group}".format(name=self.bloomberg_symbol, group=self.group)

    def update_reference(self, field, value):
        if field.name not in self._ref.keys():
            self._ref[field.name] = _SymbolReference(_field_id=field.id, _symbol_id=self.id, content=value)
        else:
            self._ref[field.name].content = value


class Timeseries(Base):
    __tablename__ = 'ts_name'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(50))
    _symbol_id = Column("symbol_id", Integer, ForeignKey('symbolsapp_symbol.id'))
    symbol = relationship("Symbol", back_populates="timeseries")

    data = relationship("_TimeseriesData", collection_class=attribute_mapped_collection('date'), back_populates="ts",
                        cascade="all, delete-orphan")
    UniqueConstraint('symbol', 'name')

    def __init__(self, name, symbol):
        self.name = name
        self.symbol = symbol

    @property
    def series(self):
        return pd.Series({pd.Timestamp(date): x.value for date, x in self.data.items()})

    def __repr__(self):
        return "{name} for {symbol}".format(name=self.name, symbol=self.symbol)

    @property
    def empty(self):
        return len(self.data) == 0

    @property
    def last_valid(self):
        if self.empty:
            return None
        else:
            return max(x for x in self.data.keys())

    def upsert(self, ts):
        for date, value in ts.items():
            if date in self.data.keys():
                # thes is some data
                self.data[date].value = value
            else:
                self.data[date] = _TimeseriesData(date=date, value=value, _ts_id=self.id)


class _TimeseriesData(Base):
    __tablename__ = 'ts_data'
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date)
    value = Column(Float)
    _ts_id = Column("ts_id", Integer, ForeignKey('ts_name.id'))
    ts = relationship("Timeseries", back_populates="data")
    UniqueConstraint("date", "ts")


class PortfolioSQL(Base):
    __tablename__ = 'portfolio'
    name = Column(String, primary_key=True)
    weights = Column(LargeBinary)
    prices = Column(LargeBinary)
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
            return self.read(self.weights)
        except ValueError:
            return pd.DataFrame({})

    @property
    def price(self):
        try:
            return self.read(self.prices)
        except ValueError:
            return pd.DataFrame({})

    @price.setter
    def price(self, value):
        self.prices = value.to_json(orient="split", date_format="iso").encode()

    @weight.setter
    def weight(self, value):
        self.weights = value.to_json(orient="split", date_format="iso").encode()

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

    id = Column(Integer, primary_key=True, autoincrement=True)
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
