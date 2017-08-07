import warnings

import pandas as pd
from mongoengine import *

from pyutil.engine.aux import flatten, dict2frame
from pyutil.mongo.asset import Asset
from pyutil.mongo.assets import Assets


def symbol(name, upsert=False):
    if upsert:
        Symbol.objects(name=name).update_one(name=name, upsert=True)

    s = Symbol.objects(name=name).first()
    assert s, "The asset {name} is unknown".format(name=name)
    return s


def assets(names=None):
    if names:
        return Assets({name: symbol(name=name).asset for name in names})
    else:
        return Assets({s.name: s.asset for s in Symbol.objects})


def reference(names=None):
    if names:
        return pd.DataFrame({name: symbol(name=name).properties for name in names}).transpose()
    else:
        return pd.DataFrame({s.name: s.properties for s in Symbol.objects.only('name','properties')}).transpose()


# we need this is for strategies
def asset(name):
    return symbol(name=name).asset


def frame(timeseries="PX_LAST", names=None):
    return assets(names=names).history[timeseries]


def names():
    return set([s.name for s in Symbol.objects.only('name')])


def bulk_update(frame):
    bulk_operations = []

    from pymongo import UpdateOne

    # update now for all assets the dynamic fields
    for asset, row in frame.iterrows():
        u = UpdateOne({"name": asset}, {'$set': flatten({"properties": row.to_dict()})})
        bulk_operations.append(u)

    Symbol._get_collection().bulk_write(bulk_operations, ordered=False)



class Symbol(Document):
    name = StringField(required=True, max_length=200, unique=True)
    properties = DictField()
    timeseries = DictField()

    @property
    def asset(self):
        return Asset(name=self.name, data=self.ts, **self.properties)

    @property
    def ts(self):
        return dict2frame(self.timeseries)

    def update_ts(self, ts, name="PX_LAST"):
        """
        Update a time series in the time series dicitionary. Index should already be a string

        :param ts: ts is a timeseries
        :param name: name of the timeseries in the timeseries dictionary
        :return:
        """
        collection = self._get_collection()
        ts = ts.dropna()
        if len(ts) > 0:
            collection.update({"name": self.name}, {"$set": flatten({"timeseries": {name: ts.to_dict()}})}, upsert=True)
        else:
            warnings.warn("No data in update for {asset}".format(asset=self.name))

        return self.reload()

    def update_ref(self, ref):
        self._get_collection().update({"name": self.name}, {"$set": flatten({"properties": ref})})
        return self.reload()


    def __repr__(self):
        return "{name}: {prop}".format(name=self.name, prop=self.properties)