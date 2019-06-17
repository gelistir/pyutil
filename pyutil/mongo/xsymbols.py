import pandas as pd
from pyutil.mongo.mongo import Collection


def prices(collection, kind="PX_LAST"):
    return pd.DataFrame({symbol["name"]: Collection.parse(symbol) for symbol in collection.find(kind=kind)})


