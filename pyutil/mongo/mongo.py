import os
import random
import string

import pandas as pd
from pymongo import MongoClient


#http://api.mongodb.com/python/current/faq.html#using-pymongo-with-multiprocessing
#It's just safer to recreate this MongoClient every time...

def collection(name=None, write=True, host=None, db=None):
    mongo = MongoClient(host=host or os.environ["MONGO_HOST"], port=27017)[db or os.environ["MONGO_DATABASE"]]
    name = name or "".join(random.choices(string.ascii_lowercase, k=10))
    return _Collection(mongo[name], write=write)


class _Collection(object):
    def __init__(self, collection, write=False):
        self.__collection = collection
        self.__write = write

    @property
    def write(self):
        return self.__write

    @write.setter
    def write(self, value):
        self.__write = value

    @property
    def name(self):
        return self.collection.name

    def upsert(self, p_obj=None, **kwargs):
        assert self.write, "It is forbidden to write into this collection"

        # check it's either unique or not there
        assert self.__collection.count_documents({**kwargs}) <= 1, "Identifier not unique"

        if p_obj is not None:
            self.__collection.update_one({**kwargs}, {"$set": {"data": p_obj.to_msgpack()}}, upsert=True)

        return p_obj

    def find(self, parse=False, **kwargs):
        for a in self.__collection.find({**kwargs}):
            yield self.__parse(a, parse=parse)

    @staticmethod
    def __parse(a, parse=True):
        if parse:
            try:
                return pd.read_msgpack(a["data"])
            except TypeError:
                return None
        else:
            return a

    def find_one(self, parse=False, **kwargs):
        assert self.__collection.count_documents({**kwargs}) <= 1, "Found multiple documents."
        return self.__parse(self.__collection.find_one({**kwargs}), parse)

    @property
    def collection(self):
        return self.__collection

    def __repr__(self):
        return self.__collection.__repr__()

    def frame(self, key, **kwargs):
        return pd.DataFrame({x[key]: self.__parse(x) for x in self.find(**kwargs)})
