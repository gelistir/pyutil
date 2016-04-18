from pymongo import MongoClient
from pymongo.database import Database
from pyutil.mongo.reader import ArchiveReader
from pyutil.mongo.writer import ArchiveWriter


def database(name, host="quantsrv", port=27017):
    client = MongoClient(host, port=port)
    return Database(client, name)


def reader(name, logger=None, host="quantsrv", port=27017):
    return ArchiveReader(database(name, host=host, port=port), logger)


def writer(name, logger=None, host="quantsrv", port=27017):
    return ArchiveWriter(database(name, host=host, port=port), logger)
