import os

import pandas as pd
import sqlalchemy as sq
from pymongo import MongoClient
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.declarative import declared_attr, has_inherited_table
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship
from sqlalchemy.orm.collections import attribute_mapped_collection

from pyutil.mongo.mongo import create_collection as create_collection, mongo_client
from pyutil.sql.base import Base
from pyutil.sql.interfaces.ref import _ReferenceData


class HasIdMixin(object):
    @declared_attr.cascading
    def id(cls):
        if has_inherited_table(cls):
            return sq.Column(sq.ForeignKey(ProductInterface.id, onupdate="CASCADE", ondelete="CASCADE"),
                             primary_key=True)
        else:
            return sq.Column(sq.Integer, primary_key=True, autoincrement=True)


class MapperArgs(object):
    @declared_attr
    def __mapper_args__(cls):
        if has_inherited_table(cls):
            return {"polymorphic_identity": cls.__name__.lower()}
        else:
            return {"polymorphic_on": "discriminator"}


class TableName(object):
    @declared_attr
    def __tablename__(cls):
        return cls.__name__.lower()


class Mongo(object):
    _client = mongo_client()

    @declared_attr
    def __collection__(cls):
        return create_collection(name=cls.__name__.lower(), client=cls._client)

    @classmethod
    def frame(cls, collection=None, **kwargs):
        col = collection or cls.__collection__
        return col.frame(key="name", **kwargs)


class ProductInterface(TableName, HasIdMixin, MapperArgs, Mongo, Base):
    # note that the name should not be unique as Portfolio and Strategy can have the same name
    __name = sq.Column("name", sq.String(200), nullable=True)

    discriminator = sq.Column(sq.String)
    __table_args__ = (sq.UniqueConstraint('discriminator', 'name'),)

    _refdata = relationship(_ReferenceData, collection_class=attribute_mapped_collection("field"),
                            cascade="all, delete-orphan", back_populates="product",
                            foreign_keys=[_ReferenceData.product_id], lazy="select")

    reference = association_proxy('_refdata', 'value', creator=lambda field, v: _ReferenceData(field=field, content=v))

    # __col = create_collection(name=cls.__name__.lower())

    def __init__(self, name, **kwargs):
        self.__name = str(name)

        # self.__collection__ is an expensive function!
        _col = self.__collection__

    @hybrid_property
    def name(self):
        # the traditional way would be to make the __name public, but then it can be changed on the fly (which we would like to avoid)
        # if we make it a standard property stuff like session.query(Symbol).filter(Symbol.name == "Maffay").one() won't work
        # Thanks to this hybrid annotation sqlalchemy translates self.__name into proper sqlcode
        # print(session.query(Symbol).filter(Symbol.name == "Maffay"))
        return self.__name

    @property
    def reference_series(self):
        return pd.Series(dict(self.reference)).rename(index=lambda x: x.name)

    def __repr__(self):
        return "{name}".format(name=self.name)

    def __lt__(self, other):
        return self.name < other.name

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self.name == other.name

    # we want to make a set of assets, etc....
    def __hash__(self):
        return hash(self.name)

    def read(self, parse=True, **kwargs):
        return self.__collection__.find_one(parse=parse, name=self.name, **kwargs)

    def write(self, data, **kwargs):
        self.__collection__.upsert(p_obj=data, name=self.name, **kwargs)

    #def refresh(self):
    #    _col = self.__collection__
    # @property
    # def collection(self):
    #    return self.__col

    # @collection.setter
    # def collection(self, value):
    #    self.__col = value

    # @staticmethod
    # def refresh():
    #    self.__col =
    # @classmethod
    # def frame(collection=None, **kwargs):
    #    col = collection or __collection__
    #    col.frame(key="name", **kwargs)
#
#    return _collection.frame(key="name", **kwargs)
# def f(self):
