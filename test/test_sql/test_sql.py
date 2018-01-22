from unittest import TestCase

from pyutil.sql.pony import upsert
from test.config import TestEnv



class TestSQL(TestCase):
    def test_init(self):
        # attention, using different database here...
        with TestEnv() as p:
            t1 = p.database.Type(name="Typ 1", comment="No")
            t2 = p.database.Type(name="Typ 2", comment="Ja")
            self.assertEqual(t1.name, "Typ 1")
            self.assertEqual(t2.name, "Typ 2")
            self.assertEqual(t1.comment, "No")
            self.assertEqual(t2.comment, "Ja")

            t = upsert(p.database.Type, get={"name": "Typ 1"}, set={"comment": "Peter Maffay"})
            self.assertEqual(t.comment, "Peter Maffay")

            t = upsert(p.database.Type, get={"name": "Typ 3"})
            self.assertIsNotNone(p.database.Type.get(name="Typ 3"))