from datetime import date as datetype
from io import BytesIO

import numpy as np
import pandas as pd
import sqlalchemy as sq
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship
from sqlalchemy.orm.collections import attribute_mapped_collection

from pyutil.sql.base import Base


def to_pandas(x):
    if x:
        return pd.read_json(BytesIO(x).read().decode(), typ="series")
    else:
        return pd.Series({})


def from_pandas(x):
    return x.to_json().encode()


class TimeseriesKeyword(Base):
    # child has multiple parents...
    # parent
    __tablename__ = 'timeseries_keyword'
    id = sq.Column(sq.Integer, primary_key=True, autoincrement=True)

    tag = sq.Column(sq.String(50))
    value = sq.Column(sq.String(50))

    timeseries_id = sq.Column(sq.Integer, sq.ForeignKey('timeseries.id'), nullable=False)

    def __init__(self, tag, value):
        self.tag = tag
        self.value = value

    def __repr__(self):
        return str((self.tag, self.value))



class Timeseries(Base):
    # child
    __tablename__ = 'timeseries'

    id = sq.Column("id", sq.Integer, primary_key=True, autoincrement=True)

    __name = sq.Column("name", sq.String(50), nullable=False)
    __field = sq.Column("field", sq.String(50), nullable=False)
    __measurement = sq.Column("measurement", sq.String(50), nullable=False)


    _jdata = sq.Column("jdata", sq.LargeBinary, nullable=True)
    _data = relationship("TimeseriesData", collection_class=attribute_mapped_collection('date'),
                         cascade="all, delete-orphan", backref="ts")

    __keywords = relationship(TimeseriesKeyword, collection_class=attribute_mapped_collection('tag'), cascade="all, delete-orphan")

    sq.UniqueConstraint(__name, __field, __measurement)

    def __init__(self, name, field, measurement, **kwargs):
        self.__name = name
        self.__field = field
        self.__measurement = measurement

        for x,y in kwargs.items():
            self.__keywords[x] = TimeseriesKeyword(tag=x, value=y)

    @hybrid_property
    def name(self):
        return self.__name

    @hybrid_property
    def field(self):
        return self.__field

    @hybrid_property
    def measurement(self):
        return self.__measurement

    @hybrid_property
    def keywords(self):
        return {x : y.value for x, y in self.__keywords.items()}

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
                        self._data[d] = TimeseriesData(date=d, value=value, ts=self)
                    else:
                        self._data[d].value = value

        # update data
        self._jdata = from_pandas(self.__series_slow)
        return self

    @property
    def __series_slow(self):
        x = pd.Series({date: x.value for date, x in self._data.items()})
        if not x.empty:
            # we read date from database!
            x = x.rename(index=lambda a: pd.Timestamp(a)).sort_index()
            assert x.index.is_monotonic_increasing, "Index is not increasing"
            assert not x.index.has_duplicates, "Index has duplicates"
        return x


    @property
    def series(self):
        x = to_pandas(self._jdata)
        if not x.empty:
            return x.apply(float).sort_index()
        else:
            return pd.Series({})

class TimeseriesData(Base):
    __tablename__ = 'ts_data'
    date = sq.Column(sq.Date, primary_key=True)
    value = sq.Column(sq.Float)
    ts_id = sq.Column("ts_id", sq.Integer, sq.ForeignKey(Timeseries.id), primary_key=True, index=True)

    def __init__(self, date, value, ts):
        assert isinstance(date, datetype)
        self.date = date
        self.value = value
        self.ts = ts
