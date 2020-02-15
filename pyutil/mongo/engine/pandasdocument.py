import datetime
import pandas as pd

from mongoengine import *


class PandasDocument(DynamicDocument):
    meta = {'abstract': True}
    name = StringField(max_length=200, required=True, unique=True)
    reference = DictField()
    date_modified = DateTimeField(default=datetime.datetime.utcnow)

    @classmethod
    def reference_frame(cls, products=None) -> pd.DataFrame:
        products = products or cls.objects

        frame = pd.DataFrame(
            {product.name: pd.Series({key: data for key, data in product.reference.items()}) for product in
             products}).transpose()
        frame.index.name = cls.__name__.lower()
        return frame.sort_index()

    def __lt__(self, other):
        return self.name < other.name

    def __eq__(self, other):
        return self.__class__ == other.__class__ and self.name == other.name

    # we want to make a set of assets, etc....
    def __hash__(self):
        return hash(self.to_json())

    def __setattr__(self, key, value):
        if isinstance(value, pd.Series):
            DynamicDocument.__setattr__(self, key, value.to_json(orient="split"))
        elif isinstance(value, pd.DataFrame):
            DynamicDocument.__setattr__(self, key, value.to_json(orient="split"))
        else:
            DynamicDocument.__setattr__(self, key, value)

    def __getattribute__(self, item):
        if item.startswith("_"):
            return DynamicDocument.__getattribute__(self, item)

        x = DynamicDocument.__getattribute__(self, item)

        try:
            return pd.read_json(x, orient="split", typ="frame")
        except:
            pass

        try:
            return pd.read_json(x, orient="split", typ="series")
        except:
            pass

        return x

    @classmethod
    def products(cls, names=None):
        # extract symbols from database
        if names is None:
            return cls.objects
        else:
            return cls.objects(name__in=names)

    @classmethod
    def pandas_frame(cls, item, products=None) -> pd.DataFrame:
        products = products or cls.objects
        frame = pd.DataFrame({product.name: product.__pandas(item=item, default=pd.Series({})) for product in products})
        frame = frame.dropna(axis=1, how="all").transpose()
        frame.index.name = cls.__name__.lower()
        return frame.sort_index().transpose()

    def __pandas(self, item, default=None):
        try:
            obj = self.__getattribute__(item)
            if isinstance(obj, pd.Series) or isinstance(obj, pd.DataFrame):
                return obj

            raise AttributeError()
        except AttributeError:
            return default

    def __str__(self):
        return "<{type}: {name}>".format(type=self.__class__.__name__, name=self.name)

    @classmethod
    def to_dict(cls):
        return {x.name: x for x in cls.objects}
