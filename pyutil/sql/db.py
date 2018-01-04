import pandas as pd
from io import BytesIO
from pony import orm
from pony.orm import Json
from types import ModuleType

from datetime import date

from pyutil.portfolio.portfolio import Portfolio, merge
from pyutil.sql.pony import upsert

db = orm.Database()

class Type(db.Entity):
    _table_ = "symbolsapp_reference_type"
    id = orm.PrimaryKey(int, auto=True)
    name = orm.Required(str, unique=True)
    comment = orm.Optional(str)
    fields = orm.Set('Field', cascade_delete=True)


class Field(db.Entity):
    _table_ = "symbolsapp_reference_field"
    id = orm.PrimaryKey(int, auto=True)
    name = orm.Required(str, unique=True)
    type = orm.Required(Type)
    refSymbols = orm.Set('SymbolReference', cascade_delete=True)

class SymbolGroup(db.Entity):
    _table_ = "symbolsapp_group"
    id = orm.PrimaryKey(int, auto=True)
    name = orm.Required(str, unique=True)
    symbols = orm.Set('Symbol', cascade_delete=True)


class Symbol(db.Entity):
    _table_ = "symbolsapp_symbol"
    id = orm.PrimaryKey(int, auto=True)
    bloomberg_symbol = orm.Required(str, unique=True)
    internal = orm.Optional(str)
    group = orm.Required(SymbolGroup)
    ref = orm.Set('SymbolReference', cascade_delete=True)
    webpage = orm.Optional(str)
    timeseries = orm.Set('Timeseries', cascade_delete=True)

    @property
    def reference(self):
        return pd.Series({r.field.name: r.content for r in self.ref})

    @property
    def series(self):
        return pd.DataFrame({x.name: x.ts for x in self.timeseries})

class SymbolReference(db.Entity):
    _table_ = 'symbolsapp_reference_data'
    id = orm.PrimaryKey(int, auto=True)
    field = orm.Required(Field)
    symbol = orm.Required(Symbol)
    content = orm.Required(str)
    orm.composite_key(symbol, field)


class Timeseries(db.Entity):
    _table_ = 'ts_name'
    id = orm.PrimaryKey(int, auto=True)
    name = orm.Required(str)
    asset = orm.Required(Symbol)
    data = orm.Set('TimeseriesData', cascade_delete=True)
    orm.composite_key(asset, name)

    @property
    def ts(self):
        return pd.Series({pd.Timestamp(x.date): x.value for x in self.data})

    @property
    def empty(self):
        return len(self.data) == 0

    @property
    def last_valid_index(self):
        if self.empty:
            return None
        else:
            return max(x.date for x in self.data)

    def append(self, ts):
        for date, value in ts.items():
            upsert(TimeseriesData, get={"ts": self, "date": date}, set={"value": value})


class TimeseriesData(db.Entity):
    _table_ = 'ts_data'
    id = orm.PrimaryKey(int, auto=True)
    date = orm.Required(date)
    value = orm.Required(float)
    ts = orm.Required(Timeseries)
    orm.composite_key(ts, date)


class Strategy(db.Entity):
    _table_ = "strategiesapp_strategy"
    id = orm.PrimaryKey(int, auto=True)
    name = orm.Required(str, unique=True)
    active = orm.Required(bool)
    source = orm.Required(str)
    portfolio = orm.Optional("PortfolioSQL")

    #@property
    def __module(self):
        compiled = compile(self.source, '', 'exec')
        module = ModuleType("module")
        exec(compiled, module.__dict__)
        return module

    def __upsert_portfolio(self, portfolio):
        w = portfolio.weights.to_json(orient="split", date_format="iso").encode()
        p = portfolio.prices.to_json(orient="split", date_format="iso").encode()
        upsert(PortfolioSQL, get={"name": self.name}, set={"weights": w, "prices": p, "strategy": self})

    def upsert_portfolio(self, portfolio):
        if self.portfolio:
            start = portfolio.index[0]
            # truncate the existing portfolio
            x = self.portfolio.portfolio.truncate(after=start - pd.DateOffset(seconds=1))
            self.__upsert_portfolio(portfolio= merge([x, portfolio], axis=0))
        else:
            self.__upsert_portfolio(portfolio=portfolio)

    def compute_portfolio(self, reader=None):
        reader = reader or asset
        config = self.__module().Configuration(reader=reader)
        return config.portfolio

    @property
    def last_valid(self):
        if self.portfolio:
            return self.portfolio.portfolio.index[-1]
        else:
            return None

    def assets(self, reader=None):
        if self.portfolio:
            reader = reader or asset
            return self.__module().Configuration(reader=reader).assets
        else:
            return None


class PortfolioSQL(db.Entity):
    _table_ = 'portfolio'
    name = orm.PrimaryKey(str)
    weights = orm.Required(bytes)
    prices = orm.Required(bytes)
    metadata = orm.Optional(Json)
    strategy = orm.Required(Strategy)

    @property
    def portfolio(self):
        def read(x):
            json_str = BytesIO(x).read().decode()
            return pd.read_json(json_str, orient="split")

        w = read(self.weights)
        p = read(self.prices)
        return Portfolio(weights=w, prices=p)


class Frame(db.Entity):
    _table_ = 'frame'
    name = orm.PrimaryKey(str)
    data = orm.Required(bytes)
    index = orm.Required(Json)
    metadata = orm.Optional(Json)

    @property
    def frame(self):
        json_str = BytesIO(self.data).read().decode()
        return pd.read_json(json_str, orient="split").set_index(keys=self.index)


def upsert_frame(name, frame):
    return upsert(Frame, get={"name": name},
                  set={"data": frame.reset_index().to_json(orient="split", date_format="iso").encode(),
                       "index": frame.index.names})


def asset(name):
    return Symbol.get(bloomberg_symbol=name)