from unittest import TestCase

from pyutil.sql.models import Base, Symbol, PortfolioSQL, SymbolType
from pyutil.sql.session import session_test, session_scope, SessionDB


class TestHistory(TestCase):

    def test_strategy_2(self):
        with session_scope(session=session_test(meta=Base.metadata)) as session:
            db = SessionDB(session=session)

            #x = db.upsert_one(SymbolGroup, get={"name": "A"})
            #y = db.upsert_one(SymbolGroup, get={"name": "A"})
            #z = db.upsert_one(SymbolGroup, get={"name": "B"})

            #self.assertEqual(x, y)

            s1 = db.upsert_one(Symbol, get={"bloomberg_symbol": "A"})
            s2 = db.upsert_one(Symbol, get={"bloomberg_symbol": "A"})

            self.assertEqual(s1, s2)


            p = db.upsert_one(PortfolioSQL, get={"name": "Peter Maffay"})
            self.assertTrue(p.empty)

            f = db.upsert_one(Symbol, get={"bloomberg_symbol": "A"}, set={"group": SymbolType.equities})
            self.assertEqual(f.group.name, "equities")