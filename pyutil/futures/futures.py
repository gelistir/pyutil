import warnings
from operator import attrgetter

import mongoengine as db
import numpy as np
import pandas as pd

from pyutil.engine.aux import dict2frame, flatten, frame2dict
from pyutil.futures.rollmap import roll_builder


class Futures(db.Document):
    name = db.StringField(required=True, max_length=200, unique=True)
    internal = db.StringField(required=True, max_length=200)

    properties = db.DictField()
    # figis of all contracts
    # figis = db.ListField()

    timeseries = db.DictField()
    quandl = db.StringField()

    meta = {
        'indexes': ['name', 'internal']
    }

    def figis(self, only_liquid=True):
        return [c.figi for c in self.contracts(only_liquid=only_liquid)]

    @property
    def gen_month(self):
        return sorted([letter for letter in self.properties["FUT_GEN_MONTH"]])

    @property
    def ref(self):
        return pd.Series(self.properties)

    def __str__(self):
        return self.name

    def update_figis(self, figis):
        # existing contracts
        exist = set(self.figis(only_liquid=False))

        # let's compute the set of new contracts
        new_contracts = set(figis).difference(exist)

        return list(new_contracts)

    def contracts(self, only_liquid=True):
        cs = sorted(Contracts.objects(future = self), key=attrgetter("notice"))

        if only_liquid:
            return [c for c in cs if c.month_x in self.gen_month]

        return cs

    def frame(self, name):
        contracts = self.contracts(only_liquid=True)
        contracts = [c for c in contracts if name in c.timeseries.keys()]
        frame = dict2frame({c.figi: c.timeseries[name] for c in contracts})
        # order the columns in order of the contracts
        return frame[[c.figi for c in contracts]]

    def roll_map(self, only_liquid=True, offset_days=0, offset_months=0):
        """
        Always roll n business days prior to expiry
        :param cons: list of contracts (have to be ordered)
        :param offset_days: the number of days
        :return:
        """
        contracts = self.contracts(only_liquid=only_liquid)
        return roll_builder(contracts, offset_days=offset_days, offset_months=offset_months)

    def roll(self, map, adjustment="N", name="PX_LAST"):

        f = self.frame(name)

        # f = frame(contracts=map.contracts, name=name)
        # the continous series
        continuous = pd.Series(index=f.index).truncate(before=map.dates[0])

        a = dict()
        a["N"] = lambda x: x.dropna()
        a["D"] = lambda x: x.dropna().diff()
        a["R"] = lambda x: 1 + x.dropna().pct_change()

        fct = a[adjustment]
        for roll_date, roll_into in map.items():
            if roll_into.figi in f.keys():
                continuous[continuous.index > roll_date] = np.nan
                s = fct(f[roll_into.figi])
                s = s[s.index > roll_date]
                continuous[s.index] = s
                current_contract = roll_into
            else:
                warnings.warn("The contract {0} is not in the ts frame".format(roll_into.properties))

            price = f[current_contract.figi][continuous.index[-1]]

        if adjustment == "N":
            return continuous

        if adjustment == "D":
            continuous[continuous.index[0]] = 0
            a = continuous.cumsum()
            return a - a[a.index[-1]] + price

        if adjustment == "R":
            continuous[continuous.index[0]] = 1
            a = continuous.cumprod()
            return price * a / a[a.index[-1]]


class Contracts(db.Document):
    figi = db.StringField(required=True, max_length=20, unique=True)
    properties = db.DictField()
    timeseries = db.DictField()
    name = db.StringField(required=True, max_length=200)
    notice = db.DateTimeField(required=True)
    future = db.ReferenceField(Futures, required=True, reverse_delete_rule=db.CASCADE)

    meta = {
        'indexes': ['figi','$figi']
        # 'ordering': ['+notice']
    }

    @db.queryset_manager
    def alive(doc_cls, queryset, today=None):
        today = today or pd.Timestamp("today")
        return queryset.filter(notice__gte = today.date())

    @property
    def month_x(self):
        months = pd.Series(index=["JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC"],
                           data=["F", "G", "H", "J", "K", "M", "N", "Q", "U", "V", "X", "Z"])

        return months[self.month_xyz]

    @property
    def month_xyz(self):
        return self.properties["FUT_MONTH_YR"][:3]

    def update_ts(self, ts, name="PX_LAST"):
        collection = self._get_collection()
        m = {"figi": self.figi}
        ts = ts.dropna()
        if len(ts) > 0:
            collection.update(m, {"$set": flatten({"timeseries": {name: ts.to_dict()}})}, upsert=True)
        else:
            warnings.warn("No data in update for {asset}".format(asset=self.figi))

        return Contracts.objects(figi=self.figi)[0]

    def update_ts_frame(self, frame):
        collection = self._get_collection()
        collection.update({"figi": self.figi}, {"$set": flatten({"timeseries": frame2dict(frame)})}, upsert=True)
        return Contracts.objects(figi=self.figi)[0]


    @property
    def ts(self):
        return dict2frame(self.timeseries)

    def last(self, name="PX_LAST"):
        if name in self.timeseries.keys():
            return self.ts[name].last_valid_index()
        else:
            # return NAT
            return pd.Timestamp("NaT")

    def __str__(self):
        return self.name

    @property
    def ref(self):
        return pd.Series(self.properties)

    @property
    def year(self):
        x = int(self.properties["FUT_MONTH_YR"][-2:])
        if x < 50:
            return x + 2000
        else:
            return x + 1900

    @property
    def quandl(self):
        return "{quandl}{month}{year}".format(quandl=self.future.quandl, month=self.month_x, year=self.year)