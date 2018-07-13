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

MEASUREMENTS = "symbols"

class Symbol(ProductInterface):
    group = sq.Column("group", _Enum(SymbolType))
    internal = sq.Column(sq.String, nullable=True)

    __mapper_args__ = {"polymorphic_identity": "symbol"}

    client = None
    measurements = "symbols"

    def __init__(self, name, group=None, internal=None):
        super().__init__(name)
        self.group = group
        self.internal = internal

    def ts(self, field="PX_LAST"):
        return Symbol.client.read_series(field=field, measurement=MEASUREMENTS, conditions={"name": self.name})

    def ts_upsert(self, ts, tags=None, field="PX_LAST"):
        """ update a series for a field """
        if not tags:
            tags = {}

        Symbol.client.write_series(field=field, measurement=MEASUREMENTS, tags={**{"name": self.name}, **tags}, ts=ts)

    # No, you can't update an entire frame for a single symbol!
    def last(self, field="PX_LAST"):
        return Symbol.client.last(measurement=MEASUREMENTS, field=field, conditions={"name": self.name})

    @staticmethod
    def read_frame(field="PX_LAST"):
        return Symbol.client.read_series(measurement=MEASUREMENTS, field=field, tags=["name"], unstack=True)

    @staticmethod
    def group_internal(symbols):
        return pd.DataFrame({symbol.name : {"group": symbol.group.name, "internal": symbol.internal} for symbol in symbols}).transpose()

    @staticmethod
    def reference_frame(symbols):
        d = dict()

        for symbol in symbols:
            x = {field.name: field.result.parse(value) for field, value in symbol.reference.items()}

            if x:
                d[symbol.name] = pd.Series(x)

        return pd.DataFrame(d).transpose().fillna("")

    @staticmethod
    def sectormap(symbols):
        return {symbol.name: symbol.group.name for symbol in symbols}


