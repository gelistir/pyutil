import pandas as pd
from mongoengine import *


class Timeseries(Document):
    data = BinaryField()
    metadata = DictField(default={})
    meta = {'allow_inheritance': True}

    @property
    def series(self):
        try:
            return pd.read_msgpack(self.data)
        except ValueError:
            return pd.Series({})

    @series.setter
    def series(self, series):
        assert isinstance(series, pd.Series)

        if self.data:
            truncated = self.series
            series = pd.concat((truncated[truncated.index < series.index[0]], series))

        self.data = series.dropna().to_msgpack()


    @property
    def last(self):
        try:
            return self.series.index[-1]
        except IndexError:
            return None


class Price(Timeseries):
    security = StringField(required=True, unique=True)


class Returns(Timeseries):
    owner = StringField(required=True, unique=True)


class Position(Timeseries):
    owner = StringField(required=True)
    security = StringField(required=True, unique_with=['owner'])


class VolatilityOwner(Timeseries):
    owner = StringField(required=True, unique=True)


class VolatilitySecurity(Timeseries):
    owner = StringField(required=True)
    security = StringField(required=True)
    currency = StringField(required=True, unique_with=['owner','security'])
