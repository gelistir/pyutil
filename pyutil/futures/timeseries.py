import warnings

import mongoengine as db
import pandas as pd
from pyutil.engine.aux import dict2frame, flatten, frame2dict


class Timeseries(db.Document):
    name = db.StringField(required=True, max_length=200, unique=True)
    properties = db.DictField()
    timeseries = db.DictField()
    quandl = db.StringField()

    meta = {
        'indexes': ['name']
    }

    @property
    def ref(self):
        return pd.Series(self.properties)

    def __str__(self):
        return self.name

    def update_ts(self, ts, name="PX_LAST"):
        collection = self._get_collection()
        m = {"name": self.name}
        ts = ts.dropna()
        if len(ts) > 0:
            collection.update(m, {"$set": flatten({"timeseries": {name: ts.to_dict()}})}, upsert=True)
        else:
            warnings.warn("No data in update for {name}".format(name=self.name))

        return Timeseries.objects(name=self.name)[0]

    def update_ts_frame(self, frame):
        collection = self._get_collection()
        collection.update({"name": self.name}, {"$set": flatten({"timeseries": frame2dict(frame)})}, upsert=True)
        return Timeseries.objects(name=self.name)[0]

    @property
    def ts(self):
        return dict2frame(self.timeseries)
