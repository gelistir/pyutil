import enum as enum

import pandas as pd
import sqlalchemy as sq
from sqlalchemy.orm import relationship
from sqlalchemy.types import Enum

from pyutil.sql.base import Base


class FieldType(enum.Enum):
    dynamic = "dynamic"
    static = "static"
    other = "other"


class DataType(enum.Enum):
    string = ("string", lambda x: x)
    integer = ("integer", lambda x: int(x))
    float = ("float", lambda x: float(x))
    date = ("date", lambda x: pd.to_datetime(int(x)*1e6).date())
    datetime = ("datetime", lambda x: pd.to_datetime(int(x)*1e6))
    percentage = ("percentage", lambda x: float(x))

    def __init__(self, value, fct):
        self.__v = value
        self.__fct = fct

    @property
    def value(self):
        # this is a bit of a hack, we are overriding the value attribute of the enum...
        # otherwise we would get everytime the tuple of name and function back...
        return self.__v

    def __call__(self, *args):
        return self.__fct(*args)


class Field(Base):
    __tablename__ = "reference_field"
    id = sq.Column("id", sq.Integer, primary_key=True, autoincrement=True)
    name = sq.Column(sq.String(50), unique=True)
    type = sq.Column(Enum(FieldType))
    result = sq.Column(Enum(DataType), nullable=False)

    def __repr__(self):
        return "({field})".format(field=self.name)

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self.name == other.name

    def __hash__(self):
        return hash(self.name)


class _ReferenceData(Base):
    __tablename__ = "reference_data"
    field_id = sq.Column("field_id", sq.Integer, sq.ForeignKey(Field.id), primary_key=True)
    field = relationship(Field)

    content = sq.Column(sq.String(200), nullable=False)

    product_id = sq.Column("product_id", sq.Integer, sq.ForeignKey("productinterface.id"), primary_key=True)
    product = relationship("ProductInterface", foreign_keys=[product_id], back_populates="_refdata")


    @property
    def value(self):
        return self.field.result(self.content)

    @value.setter
    def value(self, value):
        assert isinstance(value, str)
        self.content = value