import pandas as pd


class Leverage(object):
    def __init__(self, ts):
        self.__ts = ts

    def truncate(self, before=None, after=None):
        return Leverage(self.__ts.truncate(before, after))

    def summary(self):
        x = pd.Series()
        x["Av Leverage"] = self.__ts.mean()
        x["Current Leverage"] = self.__ts.tail(1).values[0]
        return x

    @property
    def series(self):
        return self.__ts

    def __getitem__(self, item):
        return self.__ts[item]

