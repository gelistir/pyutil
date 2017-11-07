from unittest import TestCase
from pyutil.sql.pony import upsert, db_in_memory
from test.test_sql.ponydb import Type, db


class TestSQL(TestCase):
    def test_init(self):
        with db_in_memory(db):
            t1 = Type(name="Typ 1", comment="No")
            t2 = Type(name="Typ 2", comment="Ja")
            self.assertEqual(t1.name, "Typ 1")
            self.assertEqual(t2.name, "Typ 2")
            self.assertEqual(t1.comment, "No")
            self.assertEqual(t2.comment, "Ja")

            t = upsert(Type, get={"name": "Typ 1"}, set={"comment": "Peter Maffay"})
            self.assertEqual(t.comment, "Peter Maffay")

            t = upsert(Type, get={"name": "Typ 3"})
            self.assertIsNotNone(Type.get(name="Typ 3"))