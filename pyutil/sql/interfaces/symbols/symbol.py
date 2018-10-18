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
    _measurements = "symbols"

    def __init__(self, name, group=None, internal=None):
        super().__init__(name)
        self.group = group
        self.internal = internal
