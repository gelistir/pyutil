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


class Symbol(ProductInterface):
    group = sq.Column("group", _Enum(SymbolType))
    internal = sq.Column(sq.String, nullable=True)

    __mapper_args__ = {"polymorphic_identity": "symbol"}

    def __init__(self, name, group=None, internal=None):
        super().__init__(name)
        self.group = group
        self.internal = internal

    def ts(self, client, field="px_last", date=True):
        return client.series(field=field, measurement="symbols", conditions=[("name", self.name)], date=date)

    def ts_upsert(self, client, ts, field="px_last"):
        super()._ts_upsert(client=client, field=field, series_name="symbols", tags={"name": self.name}, ts=ts)


    def last(self, client, field="px_last"):
        return client.query("""SELECT LAST({f}) FROM "symbols" where "name"='{n}'""".format(n=self.name, f=field))[
    "symbols"].index[0].date()

