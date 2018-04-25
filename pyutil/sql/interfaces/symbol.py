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
    bloomberg_symbol = sq.Column(sq.String(50), unique=True)
    group = sq.Column(_Enum(SymbolType))
    internal = sq.Column(sq.String, nullable=True)
    #id = sq.Column("id", sq.Integer, sq.ForeignKey(ProductInterface.id), primary_key=True)

    __mapper_args__ = {"polymorphic_identity": "symbol"}

    def __repr__(self):
        return "({name})".format(name=self.bloomberg_symbol)

    def __lt__(self, other):
        return self.bloomberg_symbol < other.bloomberg_symbol
