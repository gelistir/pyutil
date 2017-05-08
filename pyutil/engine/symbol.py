import collections
import pandas as pd
import warnings

from mongoengine import *
from pyutil.mongo.asset import Asset
from pyutil.mongo.assets import Assets


def assets(names=None):
    if names:
        return Assets([Symbol.objects(name=name)[0].asset for name in names])
    else:
        return Assets([s.asset for s in Symbol.objects])


class Symbol(Document):
    name = StringField(required=True, max_length=200, unique=True)
    internal = StringField(required=True, max_length=200)
    group = StringField(max_length=200)
    properties = DictField()
    timeseries = DictField()

    @staticmethod
    def __flatten(d, parent_key=None, sep='.'):
        """ flatten dictonaries of dictionaries (of dictionaries of dict... you get it)"""
        items = []
        for k, v in d.items():
            new_key = parent_key + sep + k if parent_key else k
            if isinstance(v, collections.MutableMapping):
                items.extend(Symbol.__flatten(v, new_key, sep=sep).items())
            else:
                items.append((new_key, v))
        return dict(items)

    def __hist(self):
        x = pd.DataFrame({name: pd.Series(data) for name, data in self.timeseries.items()})
        x.index = [pd.Timestamp(a) for a in x.index]
        return x

    @property
    def asset(self):
        return Asset(name=self.name, data=self.__hist(), **{**self.properties, **{"internal": self.internal, "group": self.group}})

    def update_ts(self, name, ts):
        collection = self._get_collection()
        m = {"name": self.name}
        ts = ts.dropna()
        if len(ts) > 0:
            collection.update(m, {"$set": Symbol.__flatten({"timeseries": {name: ts.to_dict()}})}, upsert=True)
            return Symbol.objects(name=self.name)[0]
        else:
            warnings.warn("No data in update for {asset}".format(asset=self.name))
