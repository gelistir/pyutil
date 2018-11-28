import pandas as pd
import sqlalchemy as sq
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship

from pyutil.sql.base import Base
from pyutil.sql.interfaces.products import ProductInterface


class Series(Base):
    __tablename__ = "series"
    __data = sq.Column("data", sq.LargeBinary)
    id = sq.Column(sq.Integer, primary_key=True, autoincrement=True)
    name = sq.Column("name", sq.String)

    product1_id = sq.Column("product1_id", sq.Integer, sq.ForeignKey(ProductInterface.id), nullable=False)
    product2_id = sq.Column("product2_id", sq.Integer, sq.ForeignKey(ProductInterface.id), nullable=True)
    product3_id = sq.Column("product3_id", sq.Integer, sq.ForeignKey(ProductInterface.id), nullable=True)


    __product1 = relationship(ProductInterface, foreign_keys=[product1_id], lazy="joined")
    __product2 = relationship(ProductInterface, foreign_keys=[product2_id], lazy="joined")
    __product3 = relationship(ProductInterface, foreign_keys=[product3_id], lazy="joined")

    @hybrid_property
    def product_2(self):
        return self.__product2


    @hybrid_property
    def product3(self):
        return self.__product3

    @hybrid_property
    def key(self):
        return self.__product2, self.__product3

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

    def __init__(self, name, product2=None, product3=None, data=None):
        self.name = name
        self.__product2 = product2
        self.__product3 = product3
        self.data = data
