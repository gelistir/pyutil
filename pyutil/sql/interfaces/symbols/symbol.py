import enum as _enum
import pandas as pd

import sqlalchemy as sq
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.types import Enum as _Enum

from pyutil.sql.interfaces.products import ProductInterface, Products


class SymbolType(_enum.Enum):
    alternatives = "Alternatives"
    fixed_income = "Fixed Income"
    currency = "Currency"
    equities = "Equities"


class Symbol(ProductInterface):
    __bloomberg_symbol = sq.Column("bloomberg_symbol", sq.String(50), unique=True)
    group = sq.Column(_Enum(SymbolType))
    internal = sq.Column(sq.String, nullable=True)

    __mapper_args__ = {"polymorphic_identity": "symbol"}

    def __init__(self, bloomberg_symbol, group=None, internal=None):
        self.__bloomberg_symbol = bloomberg_symbol
        self.group = group
        self.internal = internal

    def __repr__(self):
        return "({name})".format(name=self.bloomberg_symbol)

    def __lt__(self, other):
        return self.bloomberg_symbol < other.bloomberg_symbol

    @hybrid_property
    def bloomberg_symbol(self):
        return self.__bloomberg_symbol

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self.bloomberg_symbol == other.bloomberg_symbol

    def __hash__(self):
        return hash(self.bloomberg_symbol)


class Symbols(object):
    def __init__(self, symbols):
        for a in symbols:
            assert isinstance(a, Symbol)

        self.__symbols = {s.bloomberg_symbol: s for s in symbols}

    def __getitem__(self, item):
        return self.__symbols[item]

    def __iter__(self):
        for symbol in self.__symbols.values():
            yield symbol

    @property
    def reference(self):
        return Products(self.__symbols.values()).reference

    @hybrid_property
    def internal(self):
        return {asset: asset.internal for asset in self.__symbols.values()}

    @hybrid_property
    def group(self):
        return {asset: asset.group.name for asset in self.__symbols.values()}

    @property
    def group_internal(self):
        return pd.DataFrame({"Group": pd.Series(self.group), "Internal": pd.Series(self.internal)})

    def history(self, field="PX_LAST"):
        return Products(self.__symbols.values()).history(field=field)

    def to_dict(self):
        return self.__symbols
