import enum as _enum

import pandas as pd
import sqlalchemy as sq
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import relationship
from sqlalchemy.types import Enum as _Enum

from pyutil.performance.summary import fromNav

from pyutil.sql.interfaces.products import ProductInterface
from pyutil.sql.interfaces.series import Series
from pyutil.timeseries.merge import last_index, to_datetime, merge


class SymbolType(_enum.Enum):
    alternatives = "Alternatives"
    fixed_income = "Fixed Income"
    currency = "Currency"
    equities = "Equities"


class Symbol(ProductInterface):
    __tablename__ = "symbol"
    id = sq.Column(sq.ForeignKey(ProductInterface.id), primary_key=True)

    group = sq.Column("group", _Enum(SymbolType))
    internal = sq.Column(sq.String, nullable=True)

    __mapper_args__ = {"polymorphic_identity": "symbol"}
    _measurements = "symbols"

    # define the price...
    _price_rel = relationship(Series, uselist=False,
                           primaryjoin="and_(ProductInterface.id==Series.product1_id, Series.name=='price')")
    _price = association_proxy("_price_rel", "data", creator=lambda data: Series(name="price", data=data))

    def __init__(self, name, group, internal=None):
        super().__init__(name)
        self.group = group
        self.internal = internal

    def to_json(self):
        nav = fromNav(self._price)
        return {"name": self.name, "Price": nav, "Volatility": nav.ewm_volatility(), "Drawdown": nav.drawdown}

    def update_history(self, reader, t0=pd.Timestamp("2000-01-01"), offset=10):
        offset = pd.offsets.Day(n=offset)

        t = last_index(self._price, default=t0 + offset) - offset

        # merge new data with old existing data if it exists
        series = reader(tickers=self.name, t0=t)

        if series is not None:
            self._price = merge(new=to_datetime(series.dropna()), old=self.price)

        return self.price

    @property
    def price(self):
        return self._price

