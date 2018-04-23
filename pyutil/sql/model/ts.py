import pandas as _pd
import sqlalchemy as sq

from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.orm import relationship
from sqlalchemy.orm.collections import attribute_mapped_collection

from pyutil.sql.base import Base


# time series data for a product
class Timeseries(Base):
    __tablename__ = "ts_name"
    id = sq.Column(sq.Integer, primary_key=True, autoincrement=True)
    name = sq.Column(sq.String(100), nullable=False)
    product_id = sq.Column(sq.Integer, sq.ForeignKey("productinterface.id"))
    secondary_id = sq.Column(sq.Integer, sq.ForeignKey("productinterface.id"), nullable=True)
    secondary = relationship("ProductInterface", foreign_keys=[secondary_id])

    sq.UniqueConstraint('product', 'name', 'secondary_id')

    _data = relationship("_TimeseriesData", collection_class=attribute_mapped_collection('date'),
                         cascade="all, delete-orphan", backref="ts")

    data = association_proxy("_data", "value", creator=lambda key, value: _TimeseriesData(date=key, value=value))

    def __init__(self, name=None, product=None, data=None, secondary=None):
        self.secondary = secondary
        self.name = name
        self.product = product

        if data is not None:
            self.upsert(data)

    @property
    def series(self):
        return _pd.Series({date: x.value for date, x in self._data.items()}).sort_index()

    def upsert(self, ts=None):
        try:
            for date, value in ts.items():
                self.data[date] = value
        except AttributeError:
            pass

        return self

    @property
    def last_valid(self):
        return self.series.last_valid_index()

    @property
    def key(self):
        if self.secondary:
            return self.name, self.secondary
        else:
            return self.name


class _TimeseriesData(Base):
    __tablename__ = 'ts_data'
    date = sq.Column(sq.Date, primary_key=True)
    value = sq.Column(sq.Float)
    _ts_id = sq.Column("ts_id", sq.Integer, sq.ForeignKey(Timeseries.id), primary_key=True)

    def __init__(self, date=None, value=None, ts=None):
        self.date = date
        self.value = value
        self.ts = ts
