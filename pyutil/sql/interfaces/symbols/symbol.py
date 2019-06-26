import enum as _enum

import pandas as pd
import sqlalchemy as sq
from sqlalchemy.types import Enum as _Enum

from pyutil.sql.interfaces.products import ProductInterface
from pyutil.timeseries.merge import last_index


class SymbolType(_enum.Enum):
    alternatives = "Alternatives"
    fixed_income = "Fixed Income"
    currency = "Currency"
    equities = "Equities"


SymbolTypes = {s.value: s for s in SymbolType}


class Symbol(ProductInterface):
    __searchable__ = ['internal', 'name', 'group']

    group = sq.Column("group", _Enum(SymbolType), nullable=True)
    internal = sq.Column(sq.String, nullable=True)
    webpage = sq.Column(sq.String, nullable=True)

    def __init__(self, name, group=None, internal=None, webpage=None):
        super().__init__(name)
        self.group = group
        self.internal = internal
        self.webpage = webpage

    @staticmethod
    def reference_frame(symbols):
        frame = pd.DataFrame({symbol: {**symbol.reference_series, **{"Name": symbol.name, "Sector": symbol.group.value}} for symbol in symbols}).transpose()
        frame.index.name = "Symbol"
        frame = frame.sort_index()
        return frame

    @property
    def price(self):
        return self.read(kind="PX_LAST")

    @price.setter
    def price(self, data):
        self.write(data=data, kind="PX_LAST")

    @staticmethod
    def prices(symbols):
        return pd.DataFrame({symbol.name: symbol.price for symbol in symbols})

    def upsert_price(self, data):
        self.merge(data, kind="PX_LAST")

    @property
    def last(self):
        return last_index(self.price)
