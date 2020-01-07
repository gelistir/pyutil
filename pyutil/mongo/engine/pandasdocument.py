import datetime
import pandas as pd

from mongoengine import *


class PandasDocument(DynamicDocument):

    meta = {'allow_inheritance': True}
    name = StringField(max_length=200, required=True, unique=True)
    reference = DictField()
    date_modified = DateTimeField(default=datetime.datetime.utcnow)

    # def get_series(self, name):
    #     try:
    #         return pd.read_json(self[name], orient="split", typ="series")
    #     except (AttributeError, KeyError):
    #         return None
    #
    # def get_frame(self, name):
    #     try:
    #         return pd.read_json(self[name], orient="split", typ="frame")
    #     except (AttributeError, KeyError):
    #         return None


    @classmethod
    def reference_frame(cls, products, f=lambda x: x) -> pd.DataFrame:
        frame = pd.DataFrame({product: pd.Series({key: data for key, data in product.reference.items()}) for product in
                              products}).transpose()
        frame.index = map(f, frame.index)
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
        # todo: make the logic
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

