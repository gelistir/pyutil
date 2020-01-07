import pandas as pd

from pyutil.mongo.engine.pandasdocument import PandasDocument
from mongoengine import *


class Group(PandasDocument):
    pass


class Symbol(PandasDocument):
    group = ReferenceField(Group, required=True)
    internal = StringField(max_length=200, required=False)
    webpage = URLField(max_length=200, nullable=True)

    #def __repr__(self):
    #    return self.name, self.keys()

    @staticmethod
    def symbolmap(symbols):
        return {asset.name: asset.group.name for asset in symbols}

    @classmethod
    def reference_frame(cls, products, f=lambda x: x) -> pd.DataFrame:
        frame = PandasDocument.reference_frame(products, f)
        frame["Sector"] = pd.Series({f(symbol): symbol.group.name for symbol in products})
        frame["Internal"] = pd.Series({f(symbol): symbol.internal for symbol in products})
        print(frame)
        frame.index.name = "symbol"
        return frame