import pandas as pd
from pymongo import MongoClient

from pyutil.timeseries.merge import merge


def client(host, port=27017):
    return MongoClient(host, port)


class Collection(object):
    def __init__(self, collection):
        self.__collection = collection

    def insert(self, p_obj, **kwargs):
        # check it's either unique or not there
        n = self.__collection.count_documents({**kwargs})

        if n == 0:
            self.__collection.insert_one({"data": p_obj.to_msgpack(), **kwargs})
            return p_obj

        if n == 1:
            # do a merge... here...
            r = self.__collection.find_one({**kwargs})
            # extract the old data and merge with the new data coming in
            data = merge(old=self.parse(r["data"]), new=p_obj)
            r["data"] = data.to_msgpack()
            return data

        if n > 1:
            assert False, "Identifier not unique"

    def find(self, **kwargs):
        for a in self.__collection.find({**kwargs}):
            yield a

    def find_one(self, **kwargs):
        n = self.__collection.count_documents({**kwargs})
        assert n <= 1, "Found {n} documents".format(n=n)
        if n == 0:
            return None

        return self.__collection.find_one({**kwargs})

    @staticmethod
    def parse(x=None):
        try:
            return pd.read_msgpack(x["data"])
        except TypeError:
            return None

    @property
    def collection(self):
        return self.__collection

    def __repr__(self):
        return self.__collection.__repr__()

    def frame(self, key, **kwargs):
        return pd.DataFrame({x[key]: Collection.parse(x) for x in self.find(**kwargs)})
