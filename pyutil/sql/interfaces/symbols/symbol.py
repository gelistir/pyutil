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

    # define the price...
    #_price_rel = relationship(Series, uselist=False, primaryjoin=ProductInterface.join_series("price"))
    #_price = association_proxy("_price_rel", "data", creator=lambda data: Series(name="price", data=data))

    def __init__(self, name, group=None, internal=None, webpage=None):
        super().__init__(name)
        self.group = group
        self.internal = internal
        self.webpage = webpage

    #def upsert_price(self, ts=None):
    #    self._price = merge(new=ts, old=self.price)
    #    return self.price

    #@property
    #def price(self):
    #    return self._price

    @staticmethod
    def reference_frame(symbols):
        frame = pd.DataFrame({symbol: {**symbol.reference_series, **{"Name": symbol.name, "Sector": symbol.group.value}} for symbol in symbols}).transpose()
        frame.index.name = "Symbol"
        frame = frame.sort_index()
        return frame

    #@staticmethod
    #def history(symbols, use_name=False):
    #    if use_name:
    #        return pd.DataFrame({symbol.name: symbol.price for symbol in symbols}).dropna(how="all", axis=0)
    #    else:
    #        return pd.DataFrame({symbol: symbol.price for symbol in symbols})
