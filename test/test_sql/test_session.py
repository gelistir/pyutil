from unittest import TestCase

from pyutil.sql.models import Base, Symbol
from pyutil.sql.session import session_test, SessionDB


class TestSession(TestCase):
    def test_reference(self):
        session = session_test(meta=Base.metadata)
        db = SessionDB(session)
        self.assertEqual(db.session, session)

        s = db.upsert_one(Symbol, get={"bloomberg_symbol": "A"}, set={"internal": "Peter"})
        self.assertEqual(s.internal, "Peter")

        s = db.upsert_one(Symbol, get={"bloomberg_symbol": "A"}, set={"internal": "Maffay"})
        self.assertEqual(s.internal, "Maffay")

        x = db.dictionary(Symbol, key="bloomberg_symbol")
        self.assertDictEqual(x, {"A": s})

        self.assertIsNone(db.get(Symbol, data={"internal": "wurst"}))

        self.assertTrue(db.get(Symbol, data={"internal": "Maffay"}), s)


