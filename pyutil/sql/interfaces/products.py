import pandas as pd
import sqlalchemy as sq
from sqlalchemy.ext.associationproxy import association_proxy
from sqlalchemy.ext.declarative import declared_attr, has_inherited_table
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import relationship
from sqlalchemy.orm.collections import attribute_mapped_collection

from pyutil.mongo.mongo import create_collection as create_collection, mongo_client
from pyutil.sql.base import Base
from pyutil.sql.interfaces.ref import _ReferenceData
from pyutil.timeseries.merge import merge


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
        # this is a very fast operation, as a new client is not created here...
        return create_collection(name=cls.__name__.lower(), client=cls._client)

    #@declared_attr
    #def __collection_reference__(cls):
    #    # this is a very fast operation, as a new client is not created here...
    #    return create_collection(name=cls.__name__.lower() + "_reference", client=cls._client)

    @classmethod
    def frame(cls, products=None, **kwargs):
        if products is not None:
            return pd.DataFrame({p.name: p.read(**kwargs) for p in products})
        else:
            return cls.__collection__.frame(key="name", **kwargs)

    #@classmethod
    #def frame(cls, products, **kwargs):
    #    return pd.DataFrame({p.name : p.read(**kwargs) for p in products})
    #@classmethod
    #def meta(cls, **kwargs):
    #    return cls.__collection__.meta(**kwargs)


class ProductInterface(TableName, HasIdMixin, MapperArgs, Mongo, Base):
    # note that the name should not be unique as Portfolio and Strategy can have the same name
    __name = sq.Column("name", sq.String(200), nullable=True)

    discriminator = sq.Column(sq.String)
    __table_args__ = (sq.UniqueConstraint('discriminator', 'name'),)

    _refdata = relationship(_ReferenceData, collection_class=attribute_mapped_collection("field"),
                            cascade="all, delete-orphan", back_populates="product",
                            foreign_keys=[_ReferenceData.product_id], lazy="select")

    reference = association_proxy('_refdata', 'value', creator=lambda field, v: _ReferenceData(field=field, content=v))

    def __init__(self, name, **kwargs):
        self.__name = str(name)


    @hybrid_property
    def name(self):
        # the traditional way would be to make the __name public, but then it can be changed on the fly (which we would like to avoid)
        # if we make it a standard property stuff like session.query(Symbol).filter(Symbol.name == "Maffay").one() won't work
        # Thanks to this hybrid annotation sqlalchemy translates self.__name into proper sqlcode
        # print(session.query(Symbol).filter(Symbol.name == "Maffay"))
        return self.__name

    @property
    def reference_series(self):
        return pd.Series(dict(self.reference)).rename(index=lambda x: x.name).sort_index()

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

    def merge(self, data, **kwargs):
        old = self.read(parse=True, **kwargs)
        self.write(data=merge(new=data, old=old), **kwargs)

    def meta(self, **kwargs):
        return self.__collection__.meta(name=self.name, **kwargs)

    @classmethod
    def reference_frame(cls, products):
        # first loop over all products
        frame = pd.DataFrame({product: product.reference_series for product in products}).transpose()

        frame.index.name = cls.__name__.lower()
        return frame.sort_index()


    # def __setitem__(self, key, value):
    #     pass
    #     #self.__collection_reference__.
    #
    # def __getitem__(self, item):
    #     pass

