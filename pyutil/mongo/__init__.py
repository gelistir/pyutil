from pymongo import MongoClient

class MClient(MongoClient):
    def __init__(self, host, port=27017, **kwargs):
        super().__init__(host, port, **kwargs)


    #def list_database_names(self, session=None):

