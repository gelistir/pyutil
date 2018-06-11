import pandas as pd
import numpy as np

import pandas as _pd
import sqlalchemy as sq
from sqlalchemy.ext.hybrid import hybrid_property

from sqlalchemy.orm import relationship
from sqlalchemy.orm.collections import attribute_mapped_collection

from pyutil.sql.base import Base
from datetime import date as datetype


# time series data for a product
from pyutil.sql.util import from_pandas, to_pandas


class Timeseries(Base):
    __tablename__ = "ts_name"
    id = sq.Column(sq.Integer, primary_key=True, autoincrement=True)
    __name = sq.Column("name", sq.String(100), nullable=False)

    product_id = sq.Column(sq.Integer, sq.ForeignKey("productinterface.id"), index=True)
    product = relationship("ProductInterface", foreign_keys=[product_id], back_populates="_timeseries")

    secondary_id = sq.Column(sq.Integer, sq.ForeignKey("productinterface.id"), nullable=True)
    secondary = relationship("ProductInterface", foreign_keys=[secondary_id])

    tertiary_id = sq.Column(sq.Integer, sq.ForeignKey("productinterface.id"), nullable=True)
    tertiary = relationship("ProductInterface", foreign_keys=[tertiary_id])

    _jdata = sq.Column("jdata", sq.LargeBinary, nullable=True)
    sq.UniqueConstraint('product', 'name', 'secondary_id')

    _data = relationship("_TimeseriesData", collection_class=attribute_mapped_collection('date'),
                         cascade="all, delete-orphan", backref="ts")

    def __init__(self, name=None, product=None, data=None, secondary=None, tertiary=None):
        self.secondary = secondary
        self.tertiary = tertiary

        self.__name = name
        self.product = product

        self.upsert(data)

    @hybrid_property
    def name(self):
        return self.__name

    @property
    def __series_slow(self):
        x = _pd.Series({date: x.value for date, x in self._data.items()})
        if not x.empty:
            # we read date from database!
            x = x.rename(index=lambda a: _pd.Timestamp(a)).sort_index()
            assert x.index.is_monotonic_increasing, "Index is not increasing"
            assert not x.index.has_duplicates, "Index has duplicates"
        return x

    def upsert(self, ts=None):
        if ts is not None:
            # ts might be a dict!
            for date, value in ts.items():
                if np.isfinite(value):
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
        self._jdata = from_pandas(self.__series_slow)
        return self

    @property
    def series(self):
        x = to_pandas(self._jdata)
        if not x.empty:
            return x.apply(float).sort_index()
        else:
            return pd.Series({})

    @property
    def key(self):
        if self.tertiary:
            assert self.secondary
            return self.name, self.secondary, self.tertiary

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
