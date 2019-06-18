# import pandas as pd
# import sqlalchemy as sq
# from sqlalchemy import UniqueConstraint
# from sqlalchemy.ext.hybrid import hybrid_property
# from sqlalchemy.orm import relationship
#
# from pyutil.sql.base import Base
# from pyutil.sql.interfaces.products import ProductInterface
#
#
# class Series(Base):
#     __tablename__ = "series"
#     __data = sq.Column("data", sq.LargeBinary)
#     id = sq.Column(sq.Integer, primary_key=True, autoincrement=True)
#     name = sq.Column("name", sq.String)
#
#     _product1_id = sq.Column("product1_id", sq.Integer, sq.ForeignKey(ProductInterface.id, onupdate="CASCADE", ondelete="CASCADE"), nullable=False)
#     _product2_id = sq.Column("product2_id", sq.Integer, sq.ForeignKey(ProductInterface.id, onupdate="CASCADE", ondelete="CASCADE"), nullable=True)
#     _product3_id = sq.Column("product3_id", sq.Integer, sq.ForeignKey(ProductInterface.id, onupdate="CASCADE", ondelete="CASCADE"), nullable=True)
#
#     __product1 = relationship(ProductInterface, foreign_keys=[_product1_id], lazy="joined")
#     __product2 = relationship(ProductInterface, foreign_keys=[_product2_id], lazy="joined")
#     __product3 = relationship(ProductInterface, foreign_keys=[_product3_id], lazy="joined")
#
#     UniqueConstraint('name', 'product1_id', 'product2_id', 'product3_id', name='uix_1')
#
#     @hybrid_property
#     def product_1(self):
#         return self.__product1
#
#     @hybrid_property
#     def product_2(self):
#         return self.__product2
#
#     @hybrid_property
#     def product_3(self):
#         return self.__product3
#
#     @hybrid_property
#     def key(self):
#         return self.__product2, self.__product3
#
#     @property
#     def data(self):
#         try:
#             return pd.read_msgpack(self.__data).sort_index()
#         except ValueError:
#             return None
#
#     @data.setter
#     def data(self, series):
#         if series is not None:
#             series = series[~series.index.duplicated()]
#             self.__data = series.to_msgpack()
#
#     def __init__(self, name, product2=None, product3=None, data=None):
#         self.name = name
#         self.__product2 = product2
#         self.__product3 = product3
#         self.data = data
