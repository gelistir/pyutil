import enum as _enum

import sqlalchemy as sq
from sqlalchemy.types import Enum as _Enum

from pyutil.sql.interfaces.products import ProductInterface


class SymbolType(_enum.Enum):
    alternatives = "Alternatives"
    fixed_income = "Fixed Income"
    currency = "Currency"
    equities = "Equities"


class Symbol(ProductInterface):
    group = sq.Column("group", _Enum(SymbolType))
    internal = sq.Column(sq.String, nullable=True)

    __mapper_args__ = {"polymorphic_identity": "symbol"}

    _measurements = "symbols"

    def __init__(self, name, group=None, internal=None):
        super().__init__(name)
        self.group = group
        self.internal = internal

    def price(self, field="PX_LAST"):
        return self._ts(field=field, measurement=Symbol._measurements)

    def last(self, field="PX_LAST"):
        return self._last(field=field, measurement=Symbol._measurements)

    def upsert(self, ts, field="PX_LAST"):
        return self._ts_upsert(field=field, ts=ts, measurement=Symbol._measurements)

    @staticmethod
    def frame(field="PX_LAST"):
        return Symbol.client.read_series(measurement=Symbol._measurements, field=field, tags=["name"]).unstack(level="name")

    @staticmethod
    def symbol(name, field="PX_LAST"):
        return Symbol.client.read_series(field=field, measurement=Symbol._measurements, conditions={"name": name})
