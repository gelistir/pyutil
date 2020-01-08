import pandas as pd

from .pandasdocument import PandasDocument
from mongoengine import *


class Group(PandasDocument):
    pass


class Symbol(PandasDocument):
    group = ReferenceField(Group, required=True)
    internal = StringField(max_length=200, required=False)
    webpage = URLField(max_length=200, nullable=True)

    @staticmethod
    def symbolmap(symbols):
        return {asset.name: asset.group.name for asset in symbols}

    @classmethod
    def reference_frame(cls, products, f=lambda x: x) -> pd.DataFrame:
        frame = PandasDocument.reference_frame(products, f)
        frame["Sector"] = pd.Series({f(symbol): symbol.group.name for symbol in products})
        frame["Internal"] = pd.Series({f(symbol): symbol.internal for symbol in products})
        frame.index.name = "symbol"
        return frame

    #{"name": "AQCOMSP LX Equity",
    # "index": [1385942400000, 1386028800000, 1386115200000],
    # "data": [999.27, 998.85, 994.01]}