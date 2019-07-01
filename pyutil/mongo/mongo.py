import os
import random
import string

import pandas as pd
from pymongo import MongoClient


#http://api.mongodb.com/python/current/faq.html#using-pymongo-with-multiprocessing
def mongo_client(host=None, port=None, database=None, username=None, password=None, authSource=None):
    host = host or os.environ["MONGODB_HOST"]
    database = database or os.environ["MONGODB_DATABASE"]
    port = port or os.environ["MONGODB_PORT"]
    username = username or os.environ["MONGODB_USERNAME"]
    password = password or os.environ["MONGODB_PASSWORD"]
    authSource = authSource or os.environ["MONGODB_DATABASE"]

    print(host)
    print(database)
    print(port)
    print(username)
    print(password)
    print(authSource)

    return MongoClient(host=host, port=int(port), username=username, password=password, authSource=authSource)[database]


def create_collection(name=None, client=None):
    client = client or mongo_client()
    name = name or "".join(random.choices(string.ascii_lowercase, k=10))
    return _Collection(client[name])


class _Collection(object):
    def __init__(self, collection):
        self.__collection = collection

    @property
    def name(self):
        return self.collection.name

    def upsert(self, p_obj=None, **kwargs):
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

