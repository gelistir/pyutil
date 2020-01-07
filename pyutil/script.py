import logging
from abc import abstractmethod

#from pyutil.mongo.mongo import Mongo
from mongoengine import *

class AbstractScript(object):
    def __init__(self,  mongo_uri=None, logger=None, **kwargs):
        #self.__session = session
        self.__logger = logger or logging.getLogger(__name__)
        self.__mongo_uri = mongo_uri
        self.__dict = kwargs

    @property
    def logger(self):
        return self.__logger

    #@property
    #def session(self):
    #    return self.__session

    @abstractmethod
    def run(self):
        raise NotImplementedError

    @property
    def mongo(self):
        if self.__mongo_uri:
            # todo: think about the script

            client = connect(self.__mongo_uri, alias="default")
            assert client is not None
            yield client
            disconnect(alias="default")
            #return Mongo(uri=self.__mongo_uri).database

        return None

    def __getitem__(self, item):
        return self.__dict[item]

    @property
    def dict(self):
        return self.__dict
