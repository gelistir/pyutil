import enum as _enum

import pandas as pd
import sqlalchemy as sq
from sqlalchemy.types import Enum as _Enum

from pyutil.sql.interfaces.products import ProductInterface


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

    @staticmethod
    def read_prices(collection, kind="PX_LAST"):
        return collection.frame(key="name", kind=kind)

    def read_price(self, collection, kind="PX_LAST"):
        return collection.find_one(parse=True, kind=kind, name=self.name)

    def write_price(self, collection, data, kind="PX_LAST"):
        collection.insert(p_obj = data, kind=kind, name=self.name)
