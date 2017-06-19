from operator import attrgetter
import pandas as pd

from pyutil.container.immutable import ImmutableDict


def roll_builder(contracts, offset_days=0, offset_months=0):
    m = dict()

    contracts = sorted(contracts, key=attrgetter("notice"))

    # make sure all contracts point to the same future!
    for contract in contracts[1:]:
        assert contract.future == contracts[0].future

    # offsets
    for r_out, r_in in zip(contracts[:-1], contracts[1:]):
        m[r_out.notice - pd.offsets.DateOffset(days=offset_days, months=offset_months)] = r_in

    # make sure also the first contract is included
    m[pd.Timestamp("1900-01-01")] = contracts[0]

    return _Rollmap(m)


class _Rollmap(object):
    def __init__(self, d):
        self.__d = ImmutableDict(sorted(d.items()))

    def truncate(self, before=None, after=None):
        after = after or max(self.__d.keys())
        before = before or min(self.__d.keys())

        x = dict()

        t0 = max([t for t in self.__d.keys() if t <= before])
        x[before] = self.__d[t0]

        for t in self.__d.keys():
            if t > before and t <= after:
                x[t] = self.__d[t]

        return _Rollmap(x)

    @property
    def contracts(self):
        return [self.__d[date] for date in self.dates]

    @property
    def dates(self):
        return sorted(self.__d.keys())

    def items(self):
        for t, contract in self.__d.items():
            yield t, contract

    def __repr__(self):
        return str(pd.Series(index=self.__d.keys(), data=[self.__d[key].name for key in self.__d.keys()]))

        #return str(self.__d)
        #return str(pd.Series(self.__d))

    def __getitem__(self, item):
        return self.__d[item]
