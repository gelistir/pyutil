import pandas as pd
from sqlalchemy import String, Column
from sqlalchemy.ext.declarative import declared_attr
from sqlalchemy.ext.hybrid import hybrid_property

from pyutil.mongo.mongo import mongo_client, create_collection
from pyutil.sql.interfaces.products import Timeseries, Reference


class Product(object):

    __client = mongo_client()
    __name = Column("name", String(1000), unique=True, nullable=False)

    def __init__(self, name):
        self.__name = str(name)

    @hybrid_property
    def name(self):
        # the traditional way would be to make the __name public, but then it can be changed on the fly (which we would like to avoid)
        # if we make it a standard property stuff like session.query(Symbol).filter(Symbol.name == "Maffay").one() won't work
        # Thanks to this hybrid annotation sqlalchemy translates self.__name into proper sqlcode
        # print(session.query(Symbol).filter(Symbol.name == "Maffay"))
        return self.__name

    @declared_attr.cascading
    def collection(cls):
        # this is a very fast operation, as a new client is not created here...
        return create_collection(name=cls.__name__.lower(), client=cls.__client)

    @declared_attr.cascading
    def collection_reference(cls):
        # this is a very fast operation, as a new client is not created here...
        return create_collection(name=cls.__name__.lower() + "_reference", client=cls.__client)

    def __repr__(self):
        return "{name}".format(name=self.name)

    def __lt__(self, other):
        return self.name < other.name

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self.name == other.name

    # we want to make a set of assets, etc....
    def __hash__(self):
        return hash(self.name)

    @property
    def reference(self):
        return Reference(name=self.name, collection=self.collection_reference)

    @property
    def series(self):
        # isn't that very expensive
        return Timeseries(name=self.name, collection=self.collection)

    @classmethod
    def reference_frame(cls, products, f=lambda x: x) -> pd.DataFrame:
        frame = pd.DataFrame({product: pd.Series({key: data for key, data in product.reference.items()}) for product in products}).transpose()
        frame.index = map(f, frame.index)
        frame.index.name = cls.__name__.lower()
        return frame.sort_index()

    @classmethod
    def pandas_frame(cls, key, products, f=lambda x: x, **kwargs) -> pd.DataFrame:
        frame = pd.DataFrame({product: product.series.read(key=key, **kwargs) for product in products})
        frame = frame.dropna(axis=1, how="all").transpose()
        frame.index = map(f, frame.index)
        frame.index.name = cls.__name__.lower()
        return frame.sort_index().transpose()
