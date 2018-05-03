from io import BytesIO

import pandas as pd

import pandas as _pd
import sqlalchemy as sq
from sqlalchemy.ext.hybrid import hybrid_property

from sqlalchemy.orm import relationship
from sqlalchemy.orm.collections import attribute_mapped_collection

from pyutil.sql.base import Base
from datetime import date as datetype


# time series data for a product
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
        self.__name = name
        self.product = product

        #if data is not None:
        self.upsert(data)

    @hybrid_property
    def name(self):
        return self.__name

    @property
    def series_slow(self):
        x = _pd.Series({date: x.value for date, x in self._data.items()})
        if not x.empty:
            # we read date from database!
            x = x.rename(index=lambda a: _pd.Timestamp(a)).sort_index()
            assert x.index.is_monotonic_increasing, "Index is not increasing"
            assert not x.index.has_duplicates, "Index has duplicates"
        return x

    def upsert(self, ts=None):
        if ts is not None:
            for date, value in ts.items():
                if isinstance(date, pd.Timestamp):
                    assert date.hour == 0
                    assert date.minute == 0
                    assert date.second == 0
                    d = date.date()

                elif isinstance(date, datetype):
                    d = date

                else:
                    raise AssertionError("The index has to be a datetime or date object")

                if d not in self._data.keys():
                    self._data[d] = _TimeseriesData(date=d, value=value, ts=self)
                else:
                    self._data[d].value = value

        # update data
        x = _pd.Series({date: x.value for date, x in self._data.items()})
        if not x.empty:
            # we read date from database!
            x = x.rename(index=lambda a: _pd.Timestamp(a)).sort_index()
            assert x.index.is_monotonic_increasing, "Index is not increasing"
            assert not x.index.has_duplicates, "Index has duplicates"

        self._jdata = x.to_json().encode()
        return self

    @property
    def series_fast(self):
        try:
            # todo: apply float?
            x = pd.read_json(BytesIO(self._jdata).read().decode(), typ="series")
        except ValueError:
            x = pd.Series({})

        return x.apply(float)

    @property
    def last_valid(self):
        return self.series_fast.last_valid_index()

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

    sq.UniqueConstraint('data', 'ts_id')

    def __init__(self, date, value, ts):
        assert isinstance(date, datetype)
        self.date = date
        self.value = value
        self.ts = ts
