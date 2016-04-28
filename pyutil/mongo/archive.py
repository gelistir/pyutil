from pymongo import MongoClient
from pymongo.database import Database
from pyutil.mongo.reader import _ArchiveReader
from pyutil.mongo.writer import _ArchiveWriter


def __database(name, host="quantsrv", port=27017):
    client = MongoClient(host, port=port)
    return Database(client, name)


def reader(name, logger=None, host="quantsrv", port=27017):
    return _ArchiveReader(__database(name, host=host, port=port), logger)


def writer(name, logger=None, host="quantsrv", port=27017):
    return _ArchiveWriter(__database(name, host=host, port=port), logger)
