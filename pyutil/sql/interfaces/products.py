import pandas as pd
import sqlalchemy as sq
from sqlalchemy.ext.declarative import declared_attr, has_inherited_table
from sqlalchemy.ext.hybrid import hybrid_property

from pyutil.mongo.mongo import mongo_client, create_collection
from pyutil.sql.base import Base


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

    @declared_attr
    def __collection_reference__(cls):
        # this is a very fast operation, as a new client is not created here...
        return create_collection(name=cls.__name__.lower() + "_reference", client=cls._client)


class ProductInterface(TableName, HasIdMixin, MapperArgs, Mongo, Base):
    # note that the name should not be unique as Portfolio and Strategy can have the same name
    __name = sq.Column("name", sq.String(200), nullable=True)

    discriminator = sq.Column(sq.String)
    __table_args__ = (sq.UniqueConstraint('discriminator', 'name'),)

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
        return pd.Series({a.meta["key"]: a.data for a in self.__collection_reference__.find(name=self.name)})

    def __repr__(self):
        return "{name}".format(name=self.name)

    def __lt__(self, other):
        return self.name < other.name

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self.name == other.name

    # we want to make a set of assets, etc....
    def __hash__(self):
        return hash(self.name)

    def read(self, key, **kwargs):
        return self.__collection__.read(name=self.name, key=key, **kwargs)

    def write(self, data, key, **kwargs):
        self.__collection__.write(data=data, key=key, name=self.name, **kwargs)

    def merge(self, data, key, **kwargs):
        self.__collection__.merge(data=data, key=key, **kwargs)

    def last(self, key, **kwargs):
        try:
            return self.read(key, **kwargs).last_valid_index()
        except AttributeError:
            return None

    @classmethod
    def reference_frame(cls, products):
        frame = pd.DataFrame({product.name: product.reference_series for product in products}).transpose()
        frame.index.name = cls.__name__.lower()
        return frame.sort_index()

    @classmethod
    def pandas_frame(cls, products, key, **kwargs):
        frame = pd.DataFrame({product.name: product.read(key=key, **kwargs) for product in products}).dropna(axis=1, how="all").transpose()
        frame.index.name = cls.__name__.lower()
        return frame.sort_index().transpose()

    def __setitem__(self, key, value):
        self.__collection_reference__.upsert(name=self.name, value=value, key=key)

    def __getitem__(self, item):
        try:
            a = self.__collection_reference__.find_one(name=self.name, key=item)
            return a.data
        except AttributeError:
            return None
