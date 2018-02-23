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
    name =  Column(String(50), unique=True)
    fields = relationship("Field", backref = "type")

    def __repr__(self):
        return "Type: {type}".format(type=self.name)

class Field(Base):
    __tablename__ = "symbolsapp_reference_field"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name =  Column(String(50), unique=True)
    type_id = Column(Integer, ForeignKey('symbolsapp_reference_type.id'), nullable=True)
    data = relationship("_SymbolReference", backref = "field")

    def __repr__(self):
        return "Field: {name}, {type}".format(name=self.name, type=self.type)

class _SymbolReference(Base):
    __tablename__ = 'symbolsapp_reference_data'
    id = Column(Integer, primary_key=True, autoincrement=True)
    field_id = Column(Integer, ForeignKey('symbolsapp_reference_field.id'), nullable=False)
    symbol_id = Column(Integer, ForeignKey("symbolsapp_symbol.id"), nullable=False)
    content = Column(String(50))
    UniqueConstraint('symbol_id', 'field_id')

    def __repr__(self):
        return "{symbol}, {field}, Value: {value}".format(symbol=self.symbol, field=self.field, value=self.content)


class SymbolGroup(Base):
    __tablename__ = "symbolsapp_group"
    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(50), unique=True)
    symbols = relationship("Symbol", backref="group")

    def __repr__(self):
        return "Group: {name}".format(name=self.name)


class Symbol(Base):
    __tablename__ = "symbolsapp_symbol"
    id = Column(Integer, primary_key=True, autoincrement=True)
    bloomberg_symbol = Column(String(50), unique=True)
    group_id = Column(Integer, ForeignKey('symbolsapp_group.id'), nullable=True)
    internal = Column(String, nullable=True)

    timeseries = relationship("_Timeseries", collection_class=attribute_mapped_collection('name'), cascade="all, delete-orphan")
    ref = relationship("_SymbolReference", collection_class=attribute_mapped_collection('field.name'), cascade="all, delete-orphan")

    def __init__(self, bloomberg_symbol, group=None, timeseries=None):
        timeseries = timeseries or []
        self.bloomberg_symbol = bloomberg_symbol

        if group:
            self.group = group

        for t in timeseries:
            self.timeseries[t] = _Timeseries(name=t, symbol=self)


    @property
    def reference(self):
        return pd.Series({key: x.content for key,x in self.ref.items()})

    def __repr__(self):
        return "Symbol: {name}, {group}".format(name=self.bloomberg_symbol, group=self.group)

    def update_reference(self, field, value):
        if field.name not in self.ref.keys():
            self.ref[field.name] = _SymbolReference(field_id=field.id, symbol_id=self.id, content=value)
        else:
            self.ref[field.name].content = value


    def upsert_ts(self, name, ts):
        if name not in self.timeseries.keys():
            self.timeseries[name] = Timeseries(name=name, symbol_id=self.id)

        self.timeseries[name].upsert(ts)

class _Timeseries(Base):
    __tablename__ = 'ts_name'
    id = Column(Integer, primary_key=True, autoincrement=True)
    name =  Column(String(50))
    symbol_id = Column(Integer, ForeignKey('symbolsapp_symbol.id'))
    #symbol = relationship("Symbol", back_populates="timeseries")
    data = relationship("_TimeseriesData", collection_class=attribute_mapped_collection('date'), back_populates="ts", cascade="all, delete-orphan")
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
                self.data[date] = _TimeseriesData(date=date, value=value, ts_id=self.id)


class _TimeseriesData(Base):
    __tablename__ = 'ts_data'
    id = Column(Integer, primary_key=True, autoincrement=True)
    date = Column(Date)
    value = Column(Float)
    ts_id = Column(Integer, ForeignKey('ts_name.id'))
    ts = relationship("_Timeseries", back_populates="data")
    UniqueConstraint("date", "ts")


class PortfolioSQL(Base):

    __tablename__ = 'portfolio'
    name = Column(String, primary_key=True)
    weights = Column(LargeBinary)
    prices = Column(LargeBinary)
    strategy_id = Column(Integer, ForeignKey("strategiesapp_strategy.id"), nullable=True)
    strategy = relationship("Strategy", back_populates="portfolio")

    def __init__(self, portfolio, name, strategy=None):
        self.name = name
        if strategy:
            self.strategy = strategy
        # going through setter?
        self.weight = portfolio.weights
        # going through setter
        self.price = portfolio.prices

    @staticmethod
    def read(x):
        json_str = BytesIO(x).read().decode()
        return pd.read_json(json_str, orient="split")

    @property
    def portfolio(self):
        return Portfolio(weights=self.weight, prices=self.price)

    @property
    def weight(self):
        return self.read(self.weights)

    @property
    def price(self):
        return self.read(self.prices)

    @price.setter
    def price(self, value):
        self.prices =  value.to_json(orient="split", date_format="iso").encode()

    @weight.setter
    def weight(self, value):
        self.weights =  value.to_json(orient="split", date_format="iso").encode()

    @property
    def last_valid(self):
        return self.portfolio.index[-1]

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
        p = self.price.truncate(after=start-pd.DateOffset(seconds=1))
        w = self.weight.truncate(after=start-pd.DateOffset(seconds=1))
        self.weight = pd.concat([w, portfolio.weights], axis=0)
        self.price = pd.concat([p, portfolio.prices], axis=0)


class Strategy(Base):
    __tablename__ = "strategiesapp_strategy"

    id = Column(Integer, primary_key=True, autoincrement=True)
    name = Column(String(50), unique=True)
    active = Column(Boolean)
    source = Column(String)
    portfolio = relationship("PortfolioSQL", uselist=False, back_populates="strategy")

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
        if self.portfolio:
            return self.portfolio.assets
        else:
            return None

    def __set_portfolio(self, portfolio):
        self.portfolio = PortfolioSQL(portfolio=portfolio, strategy=self, name=self.name)

    def upsert(self, portfolio, days=5):
        if self.portfolio:
            last_valid = self.portfolio.last_valid

            # update the existing portfolio object, think about renaming upsert into update...
            self.portfolio.upsert(portfolio=portfolio.truncate(before=last_valid - pd.DateOffset(days=days)))
        else:
            # create a portfolio object for the strategy from scratch
            self.__set_portfolio(portfolio=portfolio)



class Frame(Base):

    __tablename__ = 'frame'
    name = Column(String, primary_key=True)
    data = Column(LargeBinary)
    index = Column(String)


    def __init__(self, frame, name):
        self.name = name
        print(frame.index.names)
        self.frame = frame
        #self.name = name

    @property
    def frame(self):
        json_str = BytesIO(self.data).read().decode()
        return pd.read_json(json_str, orient="split").set_index(keys=self.index.split(","))

    @frame.setter
    def frame(self, value):
        self.index = ",".join(value.index.names)
        self.data = value.reset_index().to_json(orient="split", date_format="iso").encode()


