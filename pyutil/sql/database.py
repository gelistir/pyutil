class Database(object):
    def __init__(self, session):
        self.__session = session

    @property
    def session(self):
        return self.__session
