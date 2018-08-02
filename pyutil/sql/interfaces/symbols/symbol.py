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
    return ProductInterface.client.read_series(field=field, measurement=Symbol.measurements, conditions={"name": name})


def frame(field="PX_LAST"):
    return ProductInterface.client.read_frame(measurement=Symbol.measurements, field=field, tags=["name"])


class Symbol(ProductInterface):
    group = sq.Column("group", _Enum(SymbolType))
    internal = sq.Column(sq.String, nullable=True)

    __mapper_args__ = {"polymorphic_identity": "symbol"}

    measurements = "symbols"

    def __init__(self, name, group=None, internal=None):
        super().__init__(name)
        self.group = group
        self.internal = internal

    @staticmethod
    def group_internal(symbols):
        return pd.DataFrame({s.name: {"group": s.group.name, "internal": s.internal} for s in symbols}).transpose()

    @staticmethod
    def sectormap(symbols):
        return {s.name: s.group.name for s in symbols}
