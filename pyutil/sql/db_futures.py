from pyutil.sql.session import session as sss


class Database(object):
    def __init__(self, session=None):
        self.__session = session or sss(db="futures")

