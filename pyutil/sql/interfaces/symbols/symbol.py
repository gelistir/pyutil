import enum as _enum

import pandas as pd
import sqlalchemy as sq
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import relationship
from sqlalchemy.types import Enum as _Enum

from pyutil.sql.interfaces.products import ProductInterface
from pyutil.sql.interfaces.series import Series
from pyutil.timeseries.merge import merge


class SymbolType(_enum.Enum):
    alternatives = "Alternatives"
    fixed_income = "Fixed Income"
    currency = "Currency"
    equities = "Equities"


class Symbol(ProductInterface):
    __tablename__ = "symbol"
    __searchable__ = ['internal', 'name', 'group']

    id = sq.Column(sq.ForeignKey(ProductInterface.id, onupdate="CASCADE", ondelete="CASCADE"), primary_key=True)

    group = sq.Column("group", _Enum(SymbolType))
    internal = sq.Column(sq.String, nullable=True)

    __mapper_args__ = {"polymorphic_identity": "symbol"}
    _measurements = "symbols"

    # define the price...
    _price_rel = relationship(Series, uselist=False, primaryjoin=ProductInterface.join_series("price"))
    _price = association_proxy("_price_rel", "data", creator=lambda data: Series(name="price", data=data))

    def __init__(self, name, group, internal=None):
        super().__init__(name)
        self.group = group
        self.internal = internal

    def upsert_price(self, ts=None):
        self._price = merge(new=ts, old=self.price)
        return self.price

    @property
    def price(self):
        return self._price

    @staticmethod
    def frame(symbols):
        frame = pd.DataFrame({symbol: {**symbol.reference_series, **{"Name": symbol.name, "Sector": symbol.group.value}} for symbol in symbols}).transpose()
        frame.index.name = "Symbol"
        frame = frame.sort_index()
        return frame

    @staticmethod
    def history(symbols):
        frame = pd.DataFrame({symbol: symbol.price for symbol in symbols})
        print(frame)
        return pd.DataFrame({symbol: symbol.price for symbol in symbols})
