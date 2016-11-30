from pymongo import MongoClient
from pymongo.database import Database
from .reader import _ArchiveReader



def database(name, host="quantsrv", port=27017):
    return Database(MongoClient(host, port=port), name)


def reader(name, host="quantsrv", port=27017, logger=None):
    return _ArchiveReader(database(name, host=host, port=port), logger)



