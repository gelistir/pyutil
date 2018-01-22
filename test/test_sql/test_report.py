from unittest import TestCase

from pyutil.sql.db import upsert_portfolio
from pyutil.sql.report import mtd, ytd, sector, recent, period_returns, performance
from test.config import test_portfolio, TestEnv


class TestHistory(TestCase):

    def test_strategy(self):
        with TestEnv() as p:
            p.database.SymbolGroup(name="A")
            p.database.SymbolGroup(name="B")

            for symbol in ["A", "B", "C", "D"]:
                p.database.Symbol(bloomberg_symbol=symbol, group=p.database.SymbolGroup.get(name="A"))

            for symbol in ["E", "F", "G"]:
                p.database.Symbol(bloomberg_symbol=symbol, group=p.database.SymbolGroup.get(name="B"))

            upsert_portfolio(p.database, portfolio=test_portfolio(), name="test")

            print(mtd(p.database))
            print(mtd(p.database, names=["test"]))
            print(ytd(p.database))
            print(ytd(p.database, names=["test"]))
            print(sector(p.database))
            print(recent(p.database))
            print(period_returns(p.database))
            print(performance(p.database))
