import enum as _enum
import pandas as pd

import sqlalchemy as sq
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.types import Enum as _Enum

from pyutil.sql.interfaces.products import ProductInterface, Products
from pyutil.performance.summary import fromNav


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

    def to_html_dict(self, name="PX_LAST"):
        return fromNav(ts=self.get_timeseries(name=name), adjust=False).to_dictionary()


class Symbols(Products):
    def __init__(self, symbols):
        super().__init__(symbols, cls=Symbol, attribute="name")

    @hybrid_property
    def internal(self):
        return {asset.name: asset.internal for asset in self}

    @hybrid_property
    def group(self):
        return {asset.name: asset.group.name for asset in self}

    @property
    def group_internal(self):
        # todo: fillna not working?
        return pd.DataFrame({"Group": pd.Series(self.group), "Internal": pd.Series(self.internal)})

    def to_html_dict(self):
        return self.to_html(index_name="Bloomberg")