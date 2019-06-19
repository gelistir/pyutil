import os
import pandas as pd
from pymongo import MongoClient

from pyutil.timeseries.merge import merge

_mongo = MongoClient(host=os.environ["MONGO_HOST"], port=27017)[os.environ["MONGO_DATABASE"]]


def collection(name):
    print("HAHAHA")
    return _Collection(_mongo[name])


class _Collection(object):
    def __init__(self, collection):
        self.__collection = collection

    @property
    def name(self):
        return self.collection.name

    def upsert(self, p_obj, **kwargs):
        # check it's either unique or not there
        n = self.__collection.count_documents({**kwargs})

        if n == 0:
            print("n==0")
            self.__collection.insert_one({"data": p_obj.to_msgpack(), **kwargs})
            return p_obj

        if n == 1:
            print("n==1")
            print({**kwargs})
            self.__collection.update_one({**kwargs}, {"$set": {"data": p_obj.to_msgpack()}}, upsert=False)
            return p_obj

        if n > 1:
            assert False, "Identifier not unique"

    def find(self, parse=False, **kwargs):
        for a in self.__collection.find({**kwargs}):
            if parse:
                yield _Collection.parse(a)
            else:
                yield a

    def find_one(self, parse=False, **kwargs):
        n = self.__collection.count_documents({**kwargs})
        assert n <= 1, "Found multiple {n} documents".format(n=n)
        if n == 0:
            return None

        a = self.__collection.find_one({**kwargs})

        # you may want to parse
        if parse:
            a = _Collection.parse(a)

        return a

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
        return pd.DataFrame({x[key]: _Collection.parse(x) for x in self.find(**kwargs)})
