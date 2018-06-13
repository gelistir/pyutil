import logging

from pyutil.sql.db import Database


class DatabaseFutures(Database):
    def __init__(self, session=None):
        super().__init__(db="futures", session=session)


class Runner(object):
    def __init__(self, session, logger=None):
        self.__db = DatabaseFutures(session)
        self.__logger = logger or logging.getLogger(__name__)

    @property
    def database(self):
        return self.__db

    @property
    def logger(self):
        return self.__logger