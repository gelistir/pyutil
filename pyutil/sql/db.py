import functools

import pandas as pd

from pyutil.sql.session import session as sss


class Database(object):
    def __init__(self, db, session=None):
        self.__session = session or sss(db=db)

    @property
    def session(self):
        return self.__session

    @property
    def _read(self):
        return functools.partial(pd.read_sql_query, con=self.__session.bind)