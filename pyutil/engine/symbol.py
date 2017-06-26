import warnings

import pandas as pd
from mongoengine import *

from pyutil.engine.aux import frame2dict, flatten, dict2frame
from pyutil.mongo.asset import Asset
from pyutil.mongo.assets import Assets


def assets(names=None):
    if names:
        return Assets({name: asset(name=name) for name in names})
    else:
        return Assets({s.name: s.asset for s in Symbol.objects})


# we need this is for strategies
def asset(name):
    try:
        return Symbol.objects(name=name)[0].asset
    except IndexError:
        raise IndexError("The symbol {0} is unknown".format(name))


def reference(names=None):
    if names:
        return pd.DataFrame({name: Symbol.objects(name=name)[0].properties for name in names}).transpose()
    else:
        return pd.DataFrame({s.name: s.properties for s in Symbol.objects}).transpose()


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
        collection = self._get_collection()
        m = {"name": self.name}
        ts = ts.dropna()
        if len(ts) > 0:
            collection.update(m, {"$set": flatten({"timeseries": {name: ts.to_dict()}})}, upsert=True)
        else:
            warnings.warn("No data in update for {asset}".format(asset=self.name))

        return Symbol.objects(name=self.name)[0]

    def last(self, name="PX_LAST"):
        if name in self.timeseries.keys():
            return pd.Timestamp(sorted(self.timeseries[name].keys())[-1])
        else:
            # return NAT
            return pd.Timestamp("NaT")
