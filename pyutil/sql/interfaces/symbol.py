import enum as _enum
import pandas as pd

import sqlalchemy as sq
from sqlalchemy.types import Enum as _Enum

from pyutil.sql.interfaces.products import ProductInterface, Products


class SymbolType(_enum.Enum):
    alternatives = "Alternatives"
    fixed_income = "Fixed Income"
    currency = "Currency"
    equities = "Equities"


class Symbol(ProductInterface):
    bloomberg_symbol = sq.Column(sq.String(50), unique=True)
    group = sq.Column(_Enum(SymbolType))
    internal = sq.Column(sq.String, nullable=True)

    __mapper_args__ = {"polymorphic_identity": "symbol"}

    def __repr__(self):
        return "({name})".format(name=self.bloomberg_symbol)

    def __lt__(self, other):
        return self.bloomberg_symbol < other.bloomberg_symbol


class Symbols(list):
    def __init__(self, seq):
        super().__init__(seq)
        for a in seq:
            assert isinstance(a, Symbol)

    @property
    def reference(self):
        return Products(self).reference

    @property
    def internal(self):
        return {asset: asset.internal for asset in self}

    @property
    def group(self):
        return {asset: asset.group.name for asset in self}

    @property
    def group_internal(self):
        return pd.DataFrame({"Group": pd.Series(self.group), "Internal": pd.Series(self.internal)})

    def history(self, field="PX_LAST"):
        return Products(self).history(field=field)

    def to_dict(self):
        return {asset.bloomberg_symbol: asset for asset in self}
