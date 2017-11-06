from unittest import TestCase

from pony import orm

from pyutil.sql.pony import upsert
from test.test_sql.ponydb import Type


def _db():
    from test.test_sql.ponydb import db
    orm.sql_debug(False)

    try:
        # database may already be connected
        db.bind(provider='sqlite', filename=":memory:")
        db.generate_mapping()
    except:
        #print("Problem")
        pass

    db.drop_all_tables(with_all_data=True)
    db.create_tables()
    return orm.db_session()


class TestSQL(TestCase):
    def test_init(self):
        with _db():
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