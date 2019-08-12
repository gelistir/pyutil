import enum as _enum

import pandas as pd
import sqlalchemy as sq
from sqlalchemy.types import Enum as _Enum

from pyutil.sql.base import Base
from pyutil.sql.ppp import Product


class SymbolType(_enum.Enum):
    alternatives = "Alternatives"
    fixed_income = "Fixed Income"
    currency = "Currency"
    equities = "Equities"


SymbolTypes = {s.value: s for s in SymbolType}


class Symbol(Product, Base):
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
    def reference_frame(symbols, f=lambda x: x):
        frame = Product.reference_frame(symbols, f)
        frame["Sector"] = pd.Series({f(symbol): symbol.group.value for symbol in symbols})
        frame["Internal"] = pd.Series({f(symbol): symbol.internal for symbol in symbols})
        frame.index.name = "symbol"
        return frame
