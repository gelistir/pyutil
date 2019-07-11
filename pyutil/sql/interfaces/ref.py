# import enum as enum
#
# import pandas as pd
# import sqlalchemy as sq
# from sqlalchemy.ext.hybrid import hybrid_property
# from sqlalchemy.orm import relationship
# from sqlalchemy.types import Enum
#
# from pyutil.sql.base import Base
#
#
# class FieldType(enum.Enum):
#     dynamic = "dynamic"
#     static = "static"
#     other = "other"
#
# FieldTypes = {s.value: s for s in FieldType}
#
#
# class DataType(enum.Enum):
#     string = ("string", lambda x: x)
#     integer = ("integer", lambda x: int(float(x))) # that's weird...
#     float = ("float", lambda x: float(x))
#     date = ("date", lambda x: pd.to_datetime(int(x)*1e6).date())
#     datetime = ("datetime", lambda x: pd.to_datetime(int(x)*1e6))
#
#     # need to check whether we can still sort in tables...
#     percentage = ("percentage", lambda x: "{0:.2f}%".format(float(x)))
#
#     def __init__(self, value, fct):
#         self.__v = value
#         self.__fct = fct
#
#     @property
#     def value(self):
#         # this is a bit of a hack, we are overriding the value attribute of the enum...
#         # otherwise we would get everytime the tuple of name and function back...
#         return self.__v
#
#     def __call__(self, *args):
#         return self.__fct(*args)
#
#
# DataTypes = {s.value : s for s in DataType}
#
# class Field(Base):
#     __tablename__ = "reference_field"
#     id = sq.Column("id", sq.Integer, primary_key=True, autoincrement=True)
#     __name = sq.Column("name", sq.String(50), unique=True)
#     __type = sq.Column("type", Enum(FieldType))
#     __result = sq.Column("result", Enum(DataType), nullable=False)
#     __addepar = sq.Column("addepar", sq.String(), nullable=True)
#
#
#     def __init__(self, name, result=None, type=None, addepar=None):
#         self.__name = name
#         self.__result = result or DataType.string
#         self.__type = type or FieldType.dynamic
#         self.__addepar = addepar
#
#     @hybrid_property
#     def addepar(self):
#         return self.__addepar
#
#     @hybrid_property
#     def name(self):
#         return self.__name
#
#     @hybrid_property
#     def result(self):
#         return self.__result
#
#     @hybrid_property
#     def type(self):
#         return self.__type
#
#     def __repr__(self):
#         return "({field})".format(field=self.name)
#
#     def __eq__(self, other):
#         return self.__class__ == other.__class__ and self.name == other.name
#
#     def __hash__(self):
#         return hash(self.name)
#
#     def __lt__(self, other):
#         return self.name < other.name
#
#
# class _ReferenceData(Base):
#     __tablename__ = "reference_data"
#     field_id = sq.Column("field_id", sq.Integer, sq.ForeignKey(Field.id,  onupdate="CASCADE", ondelete="CASCADE"), primary_key=True)
#     field = relationship(Field)
#
#     content = sq.Column(sq.String(200), nullable=False)
#
#     product_id = sq.Column("product_id", sq.Integer, sq.ForeignKey("productinterface.id",  onupdate="CASCADE", ondelete="CASCADE"), primary_key=True, index=True)
#     product = relationship("ProductInterface", foreign_keys=[product_id], back_populates="_refdata")
#
#     @property
#     def value(self):
#         return self.field.result(self.content)
#
#     @value.setter
#     def value(self, value):
#         self.content = str(value)
#
#     #def get(self, field, default):
#     #    try:
#     #        return self.field.result(self.content)
#         #assert field==self.field
