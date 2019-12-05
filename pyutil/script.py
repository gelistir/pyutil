import logging
from abc import abstractmethod

from pyutil.mongo.mongo import Mongo


class AbstractScript(object):
    def __init__(self, session, mongo_uri=None, logger=None, **kwargs):
        self.__session = session
        self.__logger = logger or logging.getLogger(__name__)
        self.__mongo_uri = mongo_uri
        self.__dict = kwargs

    @property
    def logger(self):
        return self.__logger

    @property
    def session(self):
        return self.__session

    @abstractmethod
    def run(self):
        raise NotImplementedError

    @property
    def mongo(self):
        if self.__mongo_uri:
            return Mongo(uri=self.__mongo_uri).database

        return None

    def __getitem__(self, item):
        return self.__dict[item]

    @property
    def dict(self):
        return self.__dict
