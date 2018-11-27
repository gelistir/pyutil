import pandas as pd
import sqlalchemy as sq
from pyutil.sql.base import Base



class Series(Base):
    __tablename__ = "series"
    __data = sq.Column("data", sq.LargeBinary)
    id = sq.Column(sq.Integer, primary_key=True, autoincrement=True)

    discriminator = sq.Column(sq.String)
    __mapper_args__ = {"polymorphic_on": discriminator}

    @property
    def data(self):
        try:
            return pd.read_msgpack(self.__data).sort_index()
        except ValueError:
            return pd.Series({})

    @data.setter
    def data(self, series):
        if series is not None:
            series = series[~series.index.duplicated()]
            self.__data = series.to_msgpack()
