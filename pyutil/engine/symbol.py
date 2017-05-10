import warnings

import pandas as pd
from mongoengine import *

from pyutil.engine.aux import frame2dict, flatten, dict2frame
from pyutil.mongo.asset import Asset
from pyutil.mongo.assets import Assets


def assets(names=None):
    if names:
        return Assets([asset(name=name) for name in names])
    else:
        return Assets([s.asset for s in Symbol.objects])


# we need this is for strategies
def asset(name):
    return Symbol.objects(name=name)[0].asset


def reference(names=None):
    if names:
        return pd.DataFrame({name: Symbol.objects(name=name)[0].properties for name in names}).transpose()
    else:
        return pd.DataFrame({s.name: s.properties for s in Symbol.objects}).transpose()


def from_asset(asset):
    return Symbol(name=asset.name, properties=asset.reference.to_dict(), timeseries = frame2dict(asset.time_series))


class Symbol(Document):
    name = StringField(required=True, max_length=200, unique=True)
    properties = DictField()
    timeseries = DictField()

    @property
    def asset(self):
        return Asset(name=self.name, data=dict2frame(self.timeseries), **self.properties)

    def update_ts(self, ts, name="PX_LAST"):
        collection = self._get_collection()
        m = {"name": self.name}
        ts = ts.dropna()
        if len(ts) > 0:
            collection.update(m, {"$set": flatten({"timeseries": {name: ts.to_dict()}})}, upsert=True)
        else:
            warnings.warn("No data in update for {asset}".format(asset=self.name))

        return Symbol.objects(name=self.name)[0]