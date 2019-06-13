from pyutil.testing.database import database
from pyutil.sql.base import Base


class TestDatabase(object):
    def test_database(self):
        db = database(base=Base)
        assert db.session
        assert db.connection
        db.session.close()

