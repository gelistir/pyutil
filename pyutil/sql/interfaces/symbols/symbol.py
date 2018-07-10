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

    def __init__(self, name, group=None, internal=None):
        super().__init__(name)
        self.group = group
        self.internal = internal

    def ts(self, client, field="px_last"):
        print(client.query("SELECT * FROM symbols"))
        return client.read_series(field=field, measurement=MEASUREMENTS, conditions=[("name", self.name)])

    def ts_upsert(self, client, ts, field="px_last"):
        """ update a series for a field """
        client.write_series(field=field, measurement=MEASUREMENTS, tags={"name": self.name}, ts=ts)

    # No, you can't update an entire frame for a single symbol!
    def last(self, client, field="px_last"):
        # todo: make this a function in the client!
        try:
            return client.query("""SELECT LAST({f}) FROM "{measurements}" where "name"='{n}'""".format(measurements=MEASUREMENTS, f=field, n=self.name))["symbols"].index[0].date()
        except KeyError:
            return None

    @staticmethod
    def read_frame(client, name="px_last"):
        try:
            return client.read_frame(measurement=MEASUREMENTS, tags=["name"]).unstack()[name]
        except KeyError:
            return pd.DataFrame({})