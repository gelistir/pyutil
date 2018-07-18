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


def symbol(name, field="PX_LAST"):
    return Symbol.client.read_series(field=field, measurement="symbols", conditions={"name": name})


class Symbol(ProductInterface):
    group = sq.Column("group", _Enum(SymbolType))
    internal = sq.Column(sq.String, nullable=True)

    __mapper_args__ = {"polymorphic_identity": "symbol"}

    measurements = "symbols"

    def __init__(self, name, group=None, internal=None):
        super().__init__(name)
        self.group = group
        self.internal = internal
        # note that self. client is inherited from ProductInterface

    def ts(self, field="PX_LAST"):
        return self.client.read_series(field=field, measurement=Symbol.measurements, conditions={"name": self.name})

    def ts_upsert(self, ts, tags=None, field="PX_LAST"):
        """ update a series for a field """
        if not tags:
            tags = {}

        self.client.write_series(field=field, measurement=Symbol.measurements, tags={**{"name": self.name}, **tags}, ts=ts)

    # No, you can't update an entire frame for a single symbol!
    def last(self, field="PX_LAST"):
        return self.client.last(measurement=Symbol.measurements, field=field, conditions={"name": self.name})

    @staticmethod
    def read_frame(field="PX_LAST"):
        return Symbol.client.read_frame(measurement=Symbol.measurements, field=field, tags=["name"])

    @staticmethod
    def group_internal(symbols):
        return pd.DataFrame({s.name: {"group": s.group.name, "internal": s.internal} for s in symbols}).transpose()

    @staticmethod
    def reference_frame(symbols):
        d = {s.name: {field.name: value for field, value in s.reference.items()} for s in symbols}
        return pd.DataFrame(d).transpose().fillna("")

    @staticmethod
    def sectormap(symbols):
        return {symbol.name: symbol.group.name for symbol in symbols}
