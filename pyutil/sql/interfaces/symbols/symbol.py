import enum as _enum
import pandas as pd

import sqlalchemy as sq
from sqlalchemy.types import Enum as _Enum

from pyutil.sql.interfaces.products import ProductInterface
from pyutil.sql.util import parse


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

    def __init__(self, name, group=None, internal=None):
        super().__init__(name)
        self.group = group
        self.internal = internal

    def ts(self, client, field="px_last"):
        return client.read_series(field=field, measurement=MEASUREMENTS, conditions={"name": self.name})

    def ts_upsert(self, client, ts, tags=None, field="px_last"):
        """ update a series for a field """
        if not tags:
            tags = {}

        client.write_series(field=field, measurement=MEASUREMENTS, tags={**{"name": self.name}, **tags}, ts=ts)

    # No, you can't update an entire frame for a single symbol!
    def last(self, client, field="px_last"):
        return client.last(measurement=MEASUREMENTS, field=field, conditions={"name": self.name})

    @staticmethod
    def read_frame(client, field="px_last"):
        return client.read_series(measurement=MEASUREMENTS, field=field, tags=["name"], unstack=True)

    @staticmethod
    def group_internal(symbols):
        return pd.DataFrame({symbol.name : {"group": symbol.group.name, "internal": symbol.internal} for symbol in symbols}).transpose()

    @staticmethod
    def reference_frame(symbols):
        def __row(symbol):
            rows = [{"symbol": symbol.name, "field": field.name, "content": value, "result": field.result} for field, value in symbol.reference.items()]
            return parse(rows, index=["symbol", "field"])

        try:
            return pd.concat([__row(symbol) for symbol in symbols], axis=0).unstack(level=-1)
        except AttributeError:
            return pd.DataFrame({})


