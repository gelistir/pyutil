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

    def __init__(self, name, group=None, internal=None):
        super().__init__(name)
        self.group = group
        self.internal = internal

    def ts(self, client, field="px_last"):
        return client.series(field=field, measurement="symbols", conditions=[("name", self.name)])

    def ts_upsert(self, client, ts, field="px_last"):
        """ update a series for a field """
        client.series_upsert(field=field, series_name="symbols", tags={"name": self.name}, ts=ts)

    def last(self, client, field="px_last"):
        try:
            return client.query("""SELECT LAST({f}) FROM "symbols" where "name"='{n}'""".format(n=self.name, f=field))["symbols"].index[0].date()
        except KeyError:
            return None
