# from unittest import TestCase
#
# from pyutil.sql.db import db, Strategy, Symbol, SymbolGroup
# from pyutil.sql.pony import db_in_memory
# from pyutil.sql.report import mtd, ytd, sector, recent, period_returns, performance
# from test.config import test_portfolio
#
#
# # class TestHistory(TestCase):
# #
# #     def test_strategy(self):
# #         with db_in_memory(db):
# #
# #             SymbolGroup(name="A")
# #             SymbolGroup(name="B")
# #
# #             for symbol in ["A", "B", "C", "D"]:
# #                 Symbol(bloomberg_symbol=symbol, group=SymbolGroup.get(name="A"))
# #
# #             for symbol in ["E", "F", "G"]:
# #                 Symbol(bloomberg_symbol=symbol, group=SymbolGroup.get(name="B"))
# #
# #             s = Strategy(name="test", source="No", active=True)
# #             s.upsert_portfolio(portfolio=test_portfolio())
# #
# #             print(mtd())
# #             print(mtd(names=["test"]))
# #             print(ytd())
# #             print(ytd(names=["test"]))
# #             print(sector())
# #             print(recent())
# #             print(period_returns())
# #             print(performance())
