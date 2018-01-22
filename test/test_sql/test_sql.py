from unittest import TestCase

from pony import orm


from pyutil.sql.pony import upsert
from test.test_sql.ponydb import define_database


class TestSQL(TestCase):
    def test_init(self):
        db = define_database(provider='sqlite', filename=":memory:")

        with orm.db_session:
            t1 = db.Type(name="Typ 1", comment="No")
            t2 = db.Type(name="Typ 2", comment="Ja")
            self.assertEqual(t1.name, "Typ 1")
            self.assertEqual(t2.name, "Typ 2")
            self.assertEqual(t1.comment, "No")
            self.assertEqual(t2.comment, "Ja")

            t = upsert(db.Type, get={"name": "Typ 1"}, set={"comment": "Peter Maffay"})
            self.assertEqual(t.comment, "Peter Maffay")

            t = upsert(db.Type, get={"name": "Typ 3"})
            self.assertIsNotNone(db.Type.get(name="Typ 3"))