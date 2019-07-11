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
        frame = ProductInterface.reference_frame(symbols)
        frame["Sector"] = pd.Series({symbol.name: symbol.group.value for symbol in symbols})
        frame["Internal"] = pd.Series({symbol.name: symbol.internal for symbol in symbols})
        frame.index.name = "symbol"
        return frame

    @property
    def price(self):
        return self.read(key="PX_LAST")

    @price.setter
    def price(self, data):
        self.write(data=data, key="PX_LAST")

    @staticmethod
    def prices(symbols):
        return Symbol.pandas_frame(products=symbols, key="PX_LAST")

    def upsert_price(self, data):
        self.merge(data, key="PX_LAST")

    @property
    def last(self):
        return last_index(self.price)
