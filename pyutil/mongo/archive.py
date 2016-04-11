from pymongo import MongoClient
from pymongo.database import Database


def database(name, host="quantsrv", port=27017):
    client = MongoClient(host, port=port)
    return Database(client, name)

