from unittest import TestCase

from pony import orm

from pyutil.sql.db import define_database, upsert_portfolio
from pyutil.sql.report import mtd, ytd, sector, recent, period_returns, performance
from test.config import test_portfolio


class TestHistory(TestCase):

    def test_strategy(self):
        db = define_database(provider='sqlite', filename=":memory:")

        with orm.db_session:
            db.SymbolGroup(name="A")
            db.SymbolGroup(name="B")

            for symbol in ["A", "B", "C", "D"]:
                db.Symbol(bloomberg_symbol=symbol, group=db.SymbolGroup.get(name="A"))

            for symbol in ["E", "F", "G"]:
                db.Symbol(bloomberg_symbol=symbol, group=db.SymbolGroup.get(name="B"))

            upsert_portfolio(db, portfolio=test_portfolio(), name="test")

            print(mtd(db))
            print(mtd(db, names=["test"]))
            print(ytd(db))
            print(ytd(db, names=["test"]))
            print(sector(db))
            print(recent(db))
            print(period_returns(db))
            print(performance(db))
