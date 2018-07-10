from datetime import date as datetype

import pandas as pd
import sqlalchemy as sq
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship
from sqlalchemy.orm.collections import attribute_mapped_collection

from pyutil.sql.base import Base
# time series data for a product
from pyutil.sql.util import to_pandas


class Timeseries(Base):
    __tablename__ = "ts_name"
    id = sq.Column(sq.Integer, primary_key=True, autoincrement=True)
    __name = sq.Column("name", sq.String(100), nullable=False)

    product_id = sq.Column(sq.Integer, sq.ForeignKey("productinterface.id"), index=True)
    product = relationship("ProductInterface", foreign_keys=[product_id], back_populates="_timeseries")

    secondary_id = sq.Column(sq.Integer, sq.ForeignKey("productinterface.id"), nullable=True)
    secondary = relationship("ProductInterface", foreign_keys=[secondary_id])

    _jdata = sq.Column("jdata", sq.LargeBinary, nullable=True)
    sq.UniqueConstraint('product', 'name', 'secondary_id')

    _data = relationship("_TimeseriesData", collection_class=attribute_mapped_collection('date'),
                         cascade="all, delete-orphan", backref="ts")

    def __init__(self, name=None, product=None, data=None, secondary=None):
        self.secondary = secondary
        #self.tertiary = tertiary

        self.__name = name
        self.product = product

        self.upsert(data)

    @hybrid_property
    def name(self):
        return self.__name

    @property
    def series(self):
        x = to_pandas(self._jdata)
        if not x.empty:
            return x.apply(float).sort_index()
        else:
            return pd.Series({})

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
    ts_id = sq.Column("ts_id", sq.Integer, sq.ForeignKey(Timeseries.id), primary_key=True, index=True)

    def __init__(self, date, value, ts):
        assert isinstance(date, datetype)
        self.date = date
        self.value = value
        self.ts = ts
