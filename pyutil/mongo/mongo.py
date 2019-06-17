import pandas as pd
from pymongo import MongoClient

def client(host, port=27017):
    return MongoClient(host, port)


class Collection(object):
    def __init__(self, collection):
        self.__collection = collection

    def insert(self, p_obj, **kwargs):
        # check it's either unique or not there
        n = self.__collection.count_documents({**kwargs})

        if n == 0:
            return self.__collection.insert_one({"data": p_obj.to_msgpack(), **kwargs})

        if n == 1:
            # do a merge... here...
            r = self.__collection.find_one({**kwargs})
            r["data"] = p_obj.to_msgpack()

        if n > 1:
            assert False, "Identifier not unique"

    def find(self, **kwargs):
        for a in self.__collection.find({**kwargs}):
            yield a

    def find_one(self, **kwargs):
        n = self.__collection.count_documents({**kwargs})
        assert n == 1, "Found {n} documents".format(n=n)
        return self.__collection.find_one({**kwargs})

    @staticmethod
    def parse(x):
        return pd.read_msgpack(x["data"])

    @property
    def collection(self):
        return self.__collection

    def __repr__(self):
        return self.__collection.__repr__()
