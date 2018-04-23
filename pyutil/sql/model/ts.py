import pandas as pd

import pandas as _pd
import sqlalchemy as sq

from sqlalchemy.orm import relationship
from sqlalchemy.orm.collections import attribute_mapped_collection

from pyutil.sql.base import Base
from datetime import date as datetype


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

    def __init__(self, name=None, product=None, data=None, secondary=None):
        self.secondary = secondary
        self.name = name
        self.product = product

        if data is not None:
            self.upsert(data)

    @property
    def series(self):
        x = _pd.Series({date: x.value for date, x in self._data.items()})
        if not x.empty:
            if not isinstance(x.index[0], _pd.Timestamp):
                x.rename(index=lambda a: _pd.Timestamp(a), inplace=True)

            assert isinstance(x.index[0], _pd.Timestamp), "Instance is {t}".format(t=type(x.index[0]))

        x = x.sort_index()
        assert x.index.is_monotonic_increasing, "Index is not increasing"
        assert not x.index.has_duplicates, "Index has duplicates"
        return x

    def upsert(self, ts=None):
        if ts is not None:
            for date, value in ts.items():
                assert isinstance(date, pd.Timestamp)
                assert date.hour == 0
                assert date.minute == 0
                assert date.second == 0
                d = date.date()
                if d not in self._data.keys():
                    self._data[d] = _TimeseriesData(date=d, value=value, ts=self)
                else:
                    self._data[d].value = value

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

    def __init__(self, date, value, ts):
        assert isinstance(date, datetype)
        self.date = date
        self.value = value
        self.ts = ts
