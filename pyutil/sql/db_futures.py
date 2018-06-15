from pyutil.sql.db import Database


class DatabaseFutures(Database):
    def __init__(self, session=None):
        super().__init__(session=session, db="futures")
