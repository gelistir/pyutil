from pymongo import MongoClient
from pymongo.database import Database
from .reader import _ArchiveReader
from .writer import _ArchiveWriter


def __database(name, host, port):
    return Database(MongoClient(host, port=port), name)


def reader(name, host, port=27017, logger=None):
    return _ArchiveReader(__database(name, host=host, port=int(port)), logger)


def writer(name, host, port=27017, logger=None):
    return _ArchiveWriter(__database(name, host=host, port=int(port)), logger)



