from pyutil.sql.db import Database


class DatabaseFutures(Database):
    def __init__(self, client, session=None):
        super().__init__(client=client, session=session, db="futures")
