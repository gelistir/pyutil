import enum as _enum

import sqlalchemy as sq
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import relationship
from sqlalchemy.types import Enum as _Enum

from pyutil.sql.interfaces.products import ProductInterface
from pyutil.performance.summary import fromNav
from pyutil.sql.interfaces.series import Series


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

    def __init__(self, name, group, internal=None):
        super().__init__(name)
        self.group = group
        self.internal = internal

    def to_json(self):
        nav = fromNav(self.price)
        return {"name": self.name, "Price": nav, "Volatility": nav.ewm_volatility(), "Drawdown": nav.drawdown}

    @property
    def last(self):
        if self.price is not None:
            return self.price.last_valid_index()
        else:
            return None


class Price(Series):
    __tablename__ = "symbol_price"
    __mapper_args__ = {"polymorphic_identity": "price"}
    id = sq.Column(sq.ForeignKey('series.id'), primary_key=True)

    __security_id = sq.Column("security_id", sq.Integer, sq.ForeignKey(Symbol.id), nullable=False)

    def __init__(self, data=None):
        self.data = data


Symbol._price = relationship(Price, uselist=False, backref="symbol")
Symbol.price = association_proxy("_price", "data", creator=lambda data: Price(data=data))