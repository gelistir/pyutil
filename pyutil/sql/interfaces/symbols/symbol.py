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
        frame = Symbol._reference_frame(symbols)
        frame["Sector"] = pd.Series({symbol: symbol.group.value for symbol in symbols})
        frame["Internal"] = pd.Series({symbol: symbol.internal for symbol in symbols})
        return frame

    @staticmethod
    def pandas_frame(symbols, key):
        return Symbol._pandas_frame(products=symbols, key=key)
