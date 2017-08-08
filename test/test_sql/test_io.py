from unittest import TestCase

from pyutil.sql.io import read_table_sql, sqlite
from test.config import resource


class TestIo(TestCase):

    def test_read(self):
        db = resource("database.db")
        f = read_table_sql(table="groups", con=sqlite(conn_str=db), index_col="name")
        assert "mdt" in f.index
