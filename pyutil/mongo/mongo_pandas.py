import pandas as pd

class MongoSeries(object):
    def __init__(self,  x):
        assert isinstance(x, pd.Series), "The argument is of type {0}. It has to be a Pandas Series".format(type(x))
        self.__series = x

    def mongo_dict(self, format="%Y%m%d", name=None):
        if name:
            return {"{name}.{time}".format(name=name, time=t.strftime(format)): v for t, v in self.__series.dropna().items()}
        else:
            return {"{time}".format(time=t.strftime(format)): v for t, v in self.__series.dropna().items()}


class MongoFrame(object):
    def __init__(self, x):
        assert isinstance(x, pd.DataFrame), "The argument is of type {0}. It has to be a Pandas DataFrame".format(type(x))
        self.__frame = x

    def mongo_dict(self, format="%Y%m%d", name=None):
        y = self.__frame.stack()
        if name:
            return {"{name}.{asset}.{time}".format(name=name, asset=t[1], time=t[0].strftime(format)): v for t, v in y.dropna().items()}
        else:
            y = self.__frame
            return {asset: MongoSeries(y[asset]).mongo_dict(format=format) for asset in y.keys()}