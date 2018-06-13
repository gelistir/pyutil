import logging


class Runner(object):
    def __init__(self, cls, session, logger=None):
        self.__session = session
        self.__db = cls(session)
        self.__logger = logger or logging.getLogger(__name__)

    @property
    def database(self):
        return self.__db

    @property
    def logger(self):
        return self.__logger

    @property
    def session(self):
        return self.__session