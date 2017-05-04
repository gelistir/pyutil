import collections
import pandas as pd
import warnings

from mongoengine import *
from pyutil.mongo.asset import Asset


def asset_builder(name):
    return Symbol.objects(name=name)[0].asset


def reference_mapping(assets, keys, mapd=None):
    mapd = mapd or map_dict()

    # extract the right reference data...
    refdata = Symbol.reference()[keys].ix[assets]

    # convert to datatypes
    for name in keys:
        if name in mapd:
            # convert the column if in the dict above
            refdata[[name]] = refdata[[name]].apply(mapd[name])

    return refdata

def map_dict():
    map_dict = dict()
    map_dict["CHG_PCT_1D"] = lambda x: pd.to_numeric(x)
    map_dict["CHG_PCT_MTD"] = lambda x: pd.to_numeric(x)
    map_dict["CHG_PCT_YTD"] = lambda x: pd.to_numeric(x)
    map_dict["PX_LAST"] = lambda x: pd.to_numeric(x)
    map_dict["PX_CLOSE_DT"] = lambda x: pd.to_datetime(1e6 * x)
    map_dict["FUND_INCEPT_DT"] = lambda x: pd.to_datetime(1e6 * x)
    map_dict["PX_VOLUME"] = lambda x: pd.to_numeric(x)
    map_dict["VOLATILITY_20D"] = lambda x: pd.to_numeric(x)
    map_dict["VOLATILITY_260D"] = lambda x: pd.to_numeric(x)
    return map_dict


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


    def __str__(self):
        return self.name + str(self.properties)

    def last(self, name="PX_LAST", day_0=pd.Timestamp("2000-01-01"), offset=pd.offsets.BDay(n=10)):
        try:
            return self.hist[name].last_valid_index() - offset
        except KeyError:
            return day_0

    @property
    def hist(self):
        def f(timeseries):
            return pd.DataFrame({name: pd.Series(data) for name, data in timeseries.items()})

        x = f(self.timeseries)
        x.index = [pd.Timestamp(a) for a in x.index]
        return x

    @property
    def asset(self):
        return Asset(name=self.name, data=self.hist, **self.ref)

    def update_ts(self, name, ts):
        collection = self._get_collection()
        m = {"name": self.name}
        ts = ts.dropna()
        if len(ts) > 0:
            collection.update(m, {"$set": Symbol.__flatten({"timeseries": {name: ts.to_dict()}})}, upsert=True)
        else:
            warnings.warn("No data in update for {asset}".format(asset=self.name))

    @property
    def ref(self):
        return {**self.properties, **{"internal": self.internal, "group": self.group}}

    @staticmethod
    def names():
        return [symbol.name for symbol in Symbol.objects]

    @staticmethod
    def reference():
        return pd.DataFrame({asset.name: asset.ref for asset in Symbol.objects}).transpose().sort_index(axis=1)